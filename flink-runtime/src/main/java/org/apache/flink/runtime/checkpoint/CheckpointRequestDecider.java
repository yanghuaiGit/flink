/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator.CheckpointTriggerRequest;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.identityHashCode;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.TOO_MANY_CHECKPOINT_REQUESTS;

/**
 * Decides whether a {@link CheckpointCoordinator.CheckpointTriggerRequest checkpoint request}
 * should be executed, dropped or postponed. Dropped requests are failed immediately. Postponed
 * requests are enqueued into a queue and can be dequeued later.
 *
 * <p>Decision is made according to:
 *
 * <ul>
 *   <li>checkpoint properties (e.g. isForce, isPeriodic)
 *   <li>checkpointing configuration (e.g. max concurrent checkpoints, min pause)
 *   <li>current state (other queued requests, pending checkpoints, last checkpoint completion time)
 * </ul>
 */
@SuppressWarnings("ConstantConditions")
class CheckpointRequestDecider {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointRequestDecider.class);
    private static final int LOG_TIME_IN_QUEUE_THRESHOLD_MS = 100;
    private static final int DEFAULT_MAX_QUEUED_REQUESTS = 1000;

    private final int maxConcurrentCheckpointAttempts;
    private final LongConsumer rescheduleTrigger;
    private final Clock clock;
    private final long minPauseBetweenCheckpoints;
    private final IntSupplier pendingCheckpointsSizeSupplier;
    private final IntSupplier numberOfCleaningCheckpointsSupplier;
    private final NavigableSet<CheckpointTriggerRequest> queuedRequests =
            new TreeSet<>(checkpointTriggerRequestsComparator());
    private final int maxQueuedRequests;

    CheckpointRequestDecider(
            int maxConcurrentCheckpointAttempts,
            LongConsumer rescheduleTrigger,
            Clock clock,
            long minPauseBetweenCheckpoints,
            IntSupplier pendingCheckpointsSizeSupplier,
            IntSupplier numberOfCleaningCheckpointsSupplier) {
        this(
                maxConcurrentCheckpointAttempts,
                rescheduleTrigger,
                clock,
                minPauseBetweenCheckpoints,
                pendingCheckpointsSizeSupplier,
                numberOfCleaningCheckpointsSupplier,
                DEFAULT_MAX_QUEUED_REQUESTS);
    }

    CheckpointRequestDecider(
            int maxConcurrentCheckpointAttempts,
            LongConsumer rescheduleTrigger,
            Clock clock,
            long minPauseBetweenCheckpoints,
            IntSupplier pendingCheckpointsSizeSupplier,
            IntSupplier numberOfCleaningCheckpointsSupplier,
            int maxQueuedRequests) {
        Preconditions.checkArgument(maxConcurrentCheckpointAttempts > 0);
        Preconditions.checkArgument(maxQueuedRequests > 0);
        this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;
        this.rescheduleTrigger = rescheduleTrigger;
        this.clock = clock;
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
        this.pendingCheckpointsSizeSupplier = pendingCheckpointsSizeSupplier;
        this.numberOfCleaningCheckpointsSupplier = numberOfCleaningCheckpointsSupplier;
        this.maxQueuedRequests = maxQueuedRequests;
    }

    /**
     * Submit a new checkpoint request and decide whether it or some other request can be executed.
     *
     * @return request that should be executed
     */
    Optional<CheckpointTriggerRequest> chooseRequestToExecute(
            CheckpointTriggerRequest newRequest, boolean isTriggering, long lastCompletionMs) {
        //当前队列里的请求数量不能大于maxQueuedRequests（写死的1000）
        // CheckpointCoordinator周期性触发的CheckpointTriggerRequest的isPeriodic一般都是true
        if (queuedRequests.size() >= maxQueuedRequests && !queuedRequests.last().isPeriodic) {
            // there are only non-periodic (ie user-submitted) requests enqueued - retain them and
            // drop the new one 直接丢弃
            newRequest.completeExceptionally(new CheckpointException(TOO_MANY_CHECKPOINT_REQUESTS));
            return Optional.empty();
        } else {
            queuedRequests.add(newRequest);
            if (queuedRequests.size() > maxQueuedRequests) {
                //移除掉最后一个
                queuedRequests
                        .pollLast()
                        .completeExceptionally(
                                new CheckpointException(TOO_MANY_CHECKPOINT_REQUESTS));
            }
            //核心逻辑 真正的去选择一个checkpoint
            Optional<CheckpointTriggerRequest> request =
                    chooseRequestToExecute(isTriggering, lastCompletionMs);
            request.ifPresent(CheckpointRequestDecider::logInQueueTime);
            return request;
        }
    }

    /**
     * Choose one of the queued requests to execute, if any.
     *
     * @return request that should be executed
     */
    Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute(
            boolean isTriggering, long lastCompletionMs) {
        Optional<CheckpointTriggerRequest> request =
                chooseRequestToExecute(isTriggering, lastCompletionMs);
        request.ifPresent(CheckpointRequestDecider::logInQueueTime);
        return request;
    }

    /**
     * Choose the next {@link CheckpointTriggerRequest request} to execute based on the provided
     * candidate and the current state. Acquires a lock and may update the state.
     *
     * @return request that should be executed
     */
    private Optional<CheckpointTriggerRequest> chooseRequestToExecute(
            boolean isTriggering, long lastCompletionMs) {
        // 如果当前正在触发checkpoint 或者 队列为空 或者 同时进行的最大checkpoint的操作数量
        //maxConcurrentCheckpointAttempts 默认是1，代表flink同一时间内只会进行一次checkpoint的操作，只有前一个checkpoint完成，才会触发下一次
        if (isTriggering
                || queuedRequests.isEmpty()
                || numberOfCleaningCheckpointsSupplier.getAsInt()
                        > maxConcurrentCheckpointAttempts) {
            return Optional.empty();
        }
        //如果pendingCheckpointsSize大于maxConcurrentCheckpointAttempts 且第一个是force的就返回，
        //checkpoint的force 都是false，savepoint的是true
        if (pendingCheckpointsSizeSupplier.getAsInt() >= maxConcurrentCheckpointAttempts) {
            return Optional.of(queuedRequests.first())
                    .filter(CheckpointTriggerRequest::isForce)
                    .map(unused -> queuedRequests.pollFirst());
        }

        CheckpointTriggerRequest first = queuedRequests.first();
        //checkpoint的isPeriodic是true，isForce是false，其实这个地方就是判断这个checkPoint是checkPoint还是savepoint
        if (!first.isForce() && first.isPeriodic) {
            //上一次checkPoint结束的时间加上2次checkPoint间隔最小时间还是大于当前时间，说明是在这个最小间隔时间内，需要重置这个调度，重新进行Schedule
            long nextTriggerDelayMillis = nextTriggerDelayMillis(lastCompletionMs);
            if (nextTriggerDelayMillis > 0) {
                queuedRequests
                        .pollFirst()
                        .completeExceptionally(
                                new CheckpointException(MINIMUM_TIME_BETWEEN_CHECKPOINTS));
                rescheduleTrigger.accept(nextTriggerDelayMillis);
                return Optional.empty();
            }
        }

        return Optional.of(queuedRequests.pollFirst());
    }

    private long nextTriggerDelayMillis(long lastCheckpointCompletionRelativeTime) {
        return lastCheckpointCompletionRelativeTime
                - clock.relativeTimeMillis()
                + minPauseBetweenCheckpoints;
    }

    @VisibleForTesting
    @Deprecated
    PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
        return new PriorityQueue<>(queuedRequests);
    }

    void abortAll(CheckpointException exception) {
        while (!queuedRequests.isEmpty()) {
            queuedRequests.pollFirst().completeExceptionally(exception);
        }
    }

    int getNumQueuedRequests() {
        return queuedRequests.size();
    }

    private static Comparator<CheckpointTriggerRequest> checkpointTriggerRequestsComparator() {
        return (r1, r2) -> {
            if (r1.props.isSavepoint() != r2.props.isSavepoint()) {
                return r1.props.isSavepoint() ? -1 : 1;
            } else if (r1.isForce() != r2.isForce()) {
                return r1.isForce() ? -1 : 1;
            } else if (r1.isPeriodic != r2.isPeriodic) {
                return r1.isPeriodic ? 1 : -1;
            } else if (r1.timestamp != r2.timestamp) {
                return Long.compare(r1.timestamp, r2.timestamp);
            } else {
                return Integer.compare(identityHashCode(r1), identityHashCode(r2));
            }
        };
    }

    private static void logInQueueTime(CheckpointTriggerRequest request) {
        if (LOG.isInfoEnabled()) {
            long timeInQueue = request.timestamp - currentTimeMillis();
            if (timeInQueue > LOG_TIME_IN_QUEUE_THRESHOLD_MS) {
                LOG.info("checkpoint request time in queue: {}", timeInQueue);
            }
        }
    }
}
