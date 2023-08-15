/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.OperatorCoordinatorCheckpointContext;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.runtime.scheduler.GlobalFailureHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@code OperatorCoordinatorHolder} holds the {@link OperatorCoordinator} and manages all its
 * interactions with the remaining components. It provides the context and is responsible for
 * checkpointing and exactly once semantics.
 *
 * <h3>Exactly-one Semantics</h3>
 *
 * <p>The semantics are described under {@link OperatorCoordinator#checkpointCoordinator(long,
 * CompletableFuture)}.
 *
 * <h3>Exactly-one Mechanism</h3>
 *
 * <p>The mechanism for exactly once semantics is as follows:
 *
 * <ul>
 *   <li>Events pass through a special channel, the {@link SubtaskGatewayImpl}. If we are not
 *       currently triggering a checkpoint, then events simply pass through.
 *   <li>With the completion of the checkpoint future for the coordinator, this subtask gateway is
 *       closed. Events coming after that are held back (buffered), because they belong to the epoch
 *       after the checkpoint.
 *   <li>Once all coordinators in the job have completed the checkpoint, the barriers to the sources
 *       are injected. If a coordinator receives a {@link AcknowledgeCheckpointEvent} from one of
 *       its subtasks, which denotes that the subtask has received the checkpoint barrier and
 *       completed checkpoint, the coordinator reopens the corresponding subtask gateway and sends
 *       out buffered events.
 *   <li>If a task fails in the meantime, the events are dropped from the gateways. From the
 *       coordinator's perspective, these events are lost, because they were sent to a failed
 *       subtask after it's latest complete checkpoint.
 * </ul>
 *
 * Thus, events delivered from coordinators behave as follows.
 *
 * <ul>
 *   <li>If the event is generated before the coordinator completes checkpoint, it would be sent out
 *       immediately.
 *   <li>If the event is generated after the coordinator completes checkpoint, it would be
 *       temporarily buffered and not be sent out to the subtask until the coordinator received a
 *       {@link AcknowledgeCheckpointEvent} from that subtask.
 *   <li>If the event is generated after the coordinator received {@link
 *       AcknowledgeCheckpointEvent}, it would be sent out immediately.
 * </ul>
 *
 * <p>This implementation can handle concurrent checkpoints. In the behavior described above, If an
 * event is generated after the coordinator has completed multiple checkpoints, and before it
 * receives {@link AcknowledgeCheckpointEvent} about any of them, the event would be buffered until
 * the coordinator has received {@link AcknowledgeCheckpointEvent} about all of these checkpoints.
 *
 * <p><b>IMPORTANT:</b> A critical assumption is that all events from the scheduler to the Tasks are
 * transported strictly in order. Events being sent from the coordinator after the checkpoint
 * barrier was injected must not overtake the checkpoint barrier. This is currently guaranteed by
 * Flink's RPC mechanism.
 *
 * <h3>Concurrency and Threading Model</h3>
 *
 * <p>This component runs strictly in the Scheduler's main-thread-executor. All calls "from the
 * outside" are either already in the main-thread-executor (when coming from Scheduler) or put into
 * the main-thread-executor (when coming from the CheckpointCoordinator). We rely on the executor to
 * preserve strict order of the calls.
 *
 * <p>Actions from the coordinator to the "outside world" (like completing a checkpoint and sending
 * an event) are also enqueued back into the scheduler main-thread executor, strictly in order.
 */
public class OperatorCoordinatorHolder
        implements OperatorCoordinatorCheckpointContext, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorCoordinatorHolder.class);

    private final OperatorCoordinator coordinator;
    private final OperatorID operatorId;
    private final LazyInitializedCoordinatorContext context;
    private final SubtaskAccess.SubtaskAccessFactory taskAccesses;

    /**
     * A map that manages subtask gateways. It is used to control the opening/closing of each
     * gateway during checkpoint. This map should only be read or modified when concurrent execution
     * attempt is disabled. Note that concurrent execution attempt is currently guaranteed to be
     * disabled when checkpoint is enabled.
     */
    private final Map<Integer, SubtaskGatewayImpl> subtaskGatewayMap;

    private final IncompleteFuturesTracker unconfirmedEvents;

    private final int operatorParallelism;
    private final int operatorMaxParallelism;

    private GlobalFailureHandler globalFailureHandler;
    private ComponentMainThreadExecutor mainThreadExecutor;

    private OperatorCoordinatorHolder(
            final OperatorID operatorId,
            final OperatorCoordinator coordinator,
            final LazyInitializedCoordinatorContext context,
            final SubtaskAccess.SubtaskAccessFactory taskAccesses,
            final int operatorParallelism,
            final int operatorMaxParallelism) {

        this.operatorId = checkNotNull(operatorId);
        this.coordinator = checkNotNull(coordinator);
        this.context = checkNotNull(context);
        this.taskAccesses = checkNotNull(taskAccesses);
        this.operatorParallelism = operatorParallelism;
        this.operatorMaxParallelism = operatorMaxParallelism;

        this.subtaskGatewayMap = new HashMap<>();
        this.unconfirmedEvents = new IncompleteFuturesTracker();
    }

    public void lazyInitialize(
            GlobalFailureHandler globalFailureHandler,
            ComponentMainThreadExecutor mainThreadExecutor) {

        this.globalFailureHandler = globalFailureHandler;
        this.mainThreadExecutor = mainThreadExecutor;

        context.lazyInitialize(globalFailureHandler, mainThreadExecutor);

        setupAllSubtaskGateways();
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public OperatorCoordinator coordinator() {
        return coordinator;
    }

    @Override
    public OperatorID operatorId() {
        return operatorId;
    }

    @Override
    public int maxParallelism() {
        return operatorMaxParallelism;
    }

    @Override
    public int currentParallelism() {
        return operatorParallelism;
    }

    // ------------------------------------------------------------------------
    //  OperatorCoordinator Interface
    // ------------------------------------------------------------------------

    public void start() throws Exception {
        mainThreadExecutor.assertRunningInMainThread();
        checkState(context.isInitialized(), "Coordinator Context is not yet initialized");
        coordinator.start();
    }

    @Override
    public void close() throws Exception {
        coordinator.close();
        context.unInitialize();
    }

    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception {
        mainThreadExecutor.assertRunningInMainThread();
        if (event instanceof AcknowledgeCheckpointEvent) {
            subtaskGatewayMap
                    .get(subtask)
                    .openGatewayAndUnmarkCheckpoint(
                            ((AcknowledgeCheckpointEvent) event).getCheckpointID());
            return;
        }
        coordinator.handleEventFromOperator(subtask, attemptNumber, event);
    }

    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        mainThreadExecutor.assertRunningInMainThread();
        coordinator.executionAttemptFailed(subtask, attemptNumber, reason);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        mainThreadExecutor.assertRunningInMainThread();

        // this needs to happen first, so that the coordinator may access the gateway
        // in the 'subtaskReset()' function (even though they cannot send events, yet).
        setupSubtaskGateway(subtask);

        coordinator.subtaskReset(subtask, checkpointId);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        // unfortunately, this method does not run in the scheduler executor, but in the
        // checkpoint coordinator time thread.
        // we can remove the delegation once the checkpoint coordinator runs fully in the
        // scheduler's main thread executor
        mainThreadExecutor.execute(() -> checkpointCoordinatorInternal(checkpointId, result));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // unfortunately, this method does not run in the scheduler executor, but in the
        // checkpoint coordinator time thread.
        // we can remove the delegation once the checkpoint coordinator runs fully in the
        // scheduler's main thread executor
        mainThreadExecutor.execute(
                () -> {
                    subtaskGatewayMap
                            .values()
                            .forEach(x -> x.openGatewayAndUnmarkCheckpoint(checkpointId));
                    coordinator.notifyCheckpointComplete(checkpointId);
                });
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // unfortunately, this method does not run in the scheduler executor, but in the
        // checkpoint coordinator time thread.
        // we can remove the delegation once the checkpoint coordinator runs fully in the
        // scheduler's main thread executor
        mainThreadExecutor.execute(
                () -> {
                    subtaskGatewayMap
                            .values()
                            .forEach(x -> x.openGatewayAndUnmarkCheckpoint(checkpointId));
                    coordinator.notifyCheckpointAborted(checkpointId);
                });
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        // the first time this method is called is early during execution graph construction,
        // before the main thread executor is set. hence this conditional check.
        if (mainThreadExecutor != null) {
            mainThreadExecutor.assertRunningInMainThread();
        }

        subtaskGatewayMap.values().forEach(SubtaskGatewayImpl::openGatewayAndUnmarkAllCheckpoint);
        context.resetFailed();

        // when initial savepoints are restored, this call comes before the mainThreadExecutor
        // is available, which is needed to set up these gateways. So during the initial restore,
        // we ignore this, and instead the gateways are set up in the "lazyInitialize" method, which
        // is called when the scheduler is properly set up.
        // this is a bit clumsy, but it is caused by the non-straightforward initialization of the
        // ExecutionGraph and Scheduler.
        if (mainThreadExecutor != null) {
            setupAllSubtaskGateways();
        }

        coordinator.resetToCheckpoint(checkpointId, checkpointData);
    }

    private void checkpointCoordinatorInternal(
            final long checkpointId, final CompletableFuture<byte[]> result) {
        mainThreadExecutor.assertRunningInMainThread();

        final CompletableFuture<byte[]> coordinatorCheckpoint = new CompletableFuture<>();

        FutureUtils.assertNoException(
                coordinatorCheckpoint.handleAsync(
                        (success, failure) -> {
                            if (failure != null) {
                                result.completeExceptionally(failure);
                            } else if (closeGateways(checkpointId)) {
                                //closeGateways是为了防止在执行检查点操作期间任务的状态发生改变。
                                //
                                //在检查点操作过程中，OperatorCoordinatorHolder要求所有关联的任务（Task）将其当前状态保存下来。然而，这个操作并非原子性的，也就是说，在保存状态的同时，任务的状态可能会因为正在处理的数据流而发生改变。这可能导致保存的状态与实际的状态不一致，从而在任务失败并恢复时引发问题。
                                //
                                //closeGateways方法通过关闭任务的输入和输出通道（也就是“闸门”）来防止这种情况的发生。具体来说，它会暂时阻止新的数据流进入任务和已处理的数据流离开任务，从而确保在执行检查点操作期间任务的状态保持不变。
                                //
                                //当检查点操作完成后，OperatorCoordinatorHolder会重新打开任务的输入和输出通道，允许数据流重新流动。这样，就可以确保检查点操作的正确性，同时最小化了对任务处理速度的影响。
                                //
                                //需要注意的是，这种关闭和重新打开任务通道的操作是透明的，也就是说，它不会影响到数据流的正常处理。任务会继续处理所有在关闭通道前已经接收到的数据，同时也会保存所有在关闭通道期间到达的新数据，等到通道重新打开后再进行处理。
                                completeCheckpointOnceEventsAreDone(checkpointId, result, success);
                            } else {
                                // if we cannot close the gateway, this means the checkpoint
                                // has been aborted before, so the future is already
                                // completed exceptionally. but we try to complete it here
                                // again, just in case, as a safety net.
                                result.completeExceptionally(
                                        new FlinkException("Cannot close gateway"));
                            }
                            return null;
                        },
                        mainThreadExecutor));

        try {
            subtaskGatewayMap.forEach(
                    (subtask, gateway) -> gateway.markForCheckpoint(checkpointId));
            coordinator.checkpointCoordinator(checkpointId, coordinatorCheckpoint);
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            result.completeExceptionally(t);
            globalFailureHandler.handleGlobalFailure(t);
        }
    }

    private boolean closeGateways(final long checkpointId) {
        int closedGateways = 0;
        for (SubtaskGatewayImpl gateway : subtaskGatewayMap.values()) {
            if (gateway.tryCloseGateway(checkpointId)) {
                closedGateways++;
            }
        }

        if (closedGateways != 0 && closedGateways != subtaskGatewayMap.values().size()) {
            throw new IllegalStateException(
                    "Some subtask gateway can be closed while others cannot. There might be a bug here.");
        }

        return closedGateways != 0;
    }

    private void completeCheckpointOnceEventsAreDone(
            final long checkpointId,
            final CompletableFuture<byte[]> checkpointFuture,
            final byte[] checkpointResult) {

        final Collection<CompletableFuture<?>> pendingEvents =
                unconfirmedEvents.getCurrentIncompleteAndReset();
        if (pendingEvents.isEmpty()) {
            checkpointFuture.complete(checkpointResult);
            return;
        }

        LOG.info(
                "Coordinator checkpoint {} for coordinator {} is awaiting {} pending events",
                checkpointId,
                operatorId,
                pendingEvents.size());

        final CompletableFuture<?> conjunct = FutureUtils.waitForAll(pendingEvents);
        conjunct.whenComplete(
                (success, failure) -> {
                    if (failure == null) {
                        checkpointFuture.complete(checkpointResult);
                    } else {
                        // if we reach this situation, then anyway the checkpoint cannot
                        // complete because
                        // (a) the target task really is down
                        // (b) we have a potentially lost RPC message and need to
                        //     do a task failover for the receiver to restore consistency
                        checkpointFuture.completeExceptionally(
                                new FlinkException(
                                        "Failing OperatorCoordinator checkpoint because some OperatorEvents "
                                                + "before this checkpoint barrier were not received by the target tasks."));
                    }
                });
    }

    // ------------------------------------------------------------------------
    //  Checkpointing Callbacks
    // ------------------------------------------------------------------------

    @Override
    public void abortCurrentTriggering() {
        // unfortunately, this method does not run in the scheduler executor, but in the
        // checkpoint coordinator time thread.
        // we can remove the delegation once the checkpoint coordinator runs fully in the
        // scheduler's main thread executor
        mainThreadExecutor.execute(
                () ->
                        subtaskGatewayMap
                                .values()
                                .forEach(
                                        SubtaskGatewayImpl
                                                ::openGatewayAndUnmarkLastCheckpointIfAny));
    }

    // ------------------------------------------------------------------------
    //  miscellaneous helpers
    // ------------------------------------------------------------------------

    private void setupAllSubtaskGateways() {
        for (int i = 0; i < operatorParallelism; i++) {
            setupSubtaskGateway(i);
        }
    }

    private void setupSubtaskGateway(int subtask) {
        for (SubtaskAccess sta : taskAccesses.getAccessesForSubtask(subtask)) {
            setupSubtaskGateway(sta);
        }
    }

    public void setupSubtaskGatewayForAttempts(int subtask, Set<Integer> attemptNumbers) {
        for (int attemptNumber : attemptNumbers) {
            setupSubtaskGateway(taskAccesses.getAccessForAttempt(subtask, attemptNumber));
        }
    }

    private void setupSubtaskGateway(final SubtaskAccess sta) {
        final SubtaskGatewayImpl gateway =
                new SubtaskGatewayImpl(sta, mainThreadExecutor, unconfirmedEvents);

        // When concurrent execution attempts is supported, the checkpoint must have been disabled.
        // Thus, we don't need to maintain subtaskGatewayMap
        if (!context.isConcurrentExecutionAttemptsSupported()) {
            subtaskGatewayMap.put(gateway.getSubtask(), gateway);
        }

        // We need to do this synchronously here, otherwise we violate the contract that
        // 'subtaskFailed()' will never overtake 'subtaskReady()'.
        // ---
        // It is also possible that by the time this method here is called, the task execution is in
        // a no-longer running state. That happens when the scheduler deals with overlapping global
        // failures and the restore method is in fact not yet restoring to the new execution
        // attempts, but still targeting the previous execution attempts (and is later subsumed
        // by another restore to the new execution attempt). This is tricky behavior that we need
        // to work around. So if the task is no longer running, we don't call the 'subtaskReady()'
        // method.
        FutureUtils.assertNoException(
                sta.hasSwitchedToRunning()
                        .thenAccept(
                                (ignored) -> {
                                    mainThreadExecutor.assertRunningInMainThread();

                                    // see bigger comment above
                                    if (sta.isStillRunning()) {
                                        notifySubtaskReady(gateway);
                                    }
                                }));
    }

    private void notifySubtaskReady(OperatorCoordinator.SubtaskGateway gateway) {
        try {
            coordinator.executionAttemptReady(
                    gateway.getSubtask(), gateway.getExecution().getAttemptNumber(), gateway);
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            globalFailureHandler.handleGlobalFailure(
                    new FlinkException("Error from OperatorCoordinator", t));
        }
    }

    // ------------------------------------------------------------------------
    //  Factories
    // ------------------------------------------------------------------------

    public static OperatorCoordinatorHolder create(
            SerializedValue<OperatorCoordinator.Provider> serializedProvider,
            ExecutionJobVertex jobVertex,
            ClassLoader classLoader,
            CoordinatorStore coordinatorStore,
            boolean supportsConcurrentExecutionAttempts)
            throws Exception {

        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            final OperatorCoordinator.Provider provider =
                    serializedProvider.deserializeValue(classLoader);
            final OperatorID opId = provider.getOperatorId();

            final SubtaskAccess.SubtaskAccessFactory taskAccesses =
                    new ExecutionSubtaskAccess.ExecutionJobVertexSubtaskAccess(jobVertex, opId);

            return create(
                    opId,
                    provider,
                    coordinatorStore,
                    jobVertex.getName(),
                    jobVertex.getGraph().getUserClassLoader(),
                    jobVertex.getParallelism(),
                    jobVertex.getMaxParallelism(),
                    taskAccesses,
                    supportsConcurrentExecutionAttempts);
        }
    }

    @VisibleForTesting
    static OperatorCoordinatorHolder create(
            final OperatorID opId,
            final OperatorCoordinator.Provider coordinatorProvider,
            final CoordinatorStore coordinatorStore,
            final String operatorName,
            final ClassLoader userCodeClassLoader,
            final int operatorParallelism,
            final int operatorMaxParallelism,
            final SubtaskAccess.SubtaskAccessFactory taskAccesses,
            final boolean supportsConcurrentExecutionAttempts)
            throws Exception {

        final LazyInitializedCoordinatorContext context =
                new LazyInitializedCoordinatorContext(
                        opId,
                        operatorName,
                        userCodeClassLoader,
                        operatorParallelism,
                        coordinatorStore,
                        supportsConcurrentExecutionAttempts);

        final OperatorCoordinator coordinator = coordinatorProvider.create(context);

        return new OperatorCoordinatorHolder(
                opId,
                coordinator,
                context,
                taskAccesses,
                operatorParallelism,
                operatorMaxParallelism);
    }

    // ------------------------------------------------------------------------
    //  Nested Classes
    // ------------------------------------------------------------------------

    /**
     * An implementation of the {@link OperatorCoordinator.Context}.
     *
     * <p>All methods are safe to be called from other threads than the Scheduler's and the
     * JobMaster's main threads.
     *
     * <p>Implementation note: Ideally, we would like to operate purely against the scheduler
     * interface, but it is not exposing enough information at the moment.
     */
    private static final class LazyInitializedCoordinatorContext
            implements OperatorCoordinator.Context {

        private static final Logger LOG =
                LoggerFactory.getLogger(LazyInitializedCoordinatorContext.class);

        private final OperatorID operatorId;
        private final String operatorName;
        private final ClassLoader userCodeClassLoader;
        private final int operatorParallelism;
        private final CoordinatorStore coordinatorStore;
        private final boolean supportsConcurrentExecutionAttempts;

        private GlobalFailureHandler globalFailureHandler;
        private Executor schedulerExecutor;

        private volatile boolean failed;

        public LazyInitializedCoordinatorContext(
                final OperatorID operatorId,
                final String operatorName,
                final ClassLoader userCodeClassLoader,
                final int operatorParallelism,
                final CoordinatorStore coordinatorStore,
                final boolean supportsConcurrentExecutionAttempts) {
            this.operatorId = checkNotNull(operatorId);
            this.operatorName = checkNotNull(operatorName);
            this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
            this.operatorParallelism = operatorParallelism;
            this.coordinatorStore = checkNotNull(coordinatorStore);
            this.supportsConcurrentExecutionAttempts = supportsConcurrentExecutionAttempts;
        }

        void lazyInitialize(GlobalFailureHandler globalFailureHandler, Executor schedulerExecutor) {
            this.globalFailureHandler = checkNotNull(globalFailureHandler);
            this.schedulerExecutor = checkNotNull(schedulerExecutor);
        }

        void unInitialize() {
            this.globalFailureHandler = null;
            this.schedulerExecutor = null;
        }

        boolean isInitialized() {
            return schedulerExecutor != null;
        }

        private void checkInitialized() {
            checkState(isInitialized(), "Context was not yet initialized");
        }

        void resetFailed() {
            failed = false;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public void failJob(final Throwable cause) {
            checkInitialized();

            final FlinkException e =
                    new FlinkException(
                            "Global failure triggered by OperatorCoordinator for '"
                                    + operatorName
                                    + "' (operator "
                                    + operatorId
                                    + ").",
                            cause);

            if (failed) {
                LOG.debug(
                        "Ignoring the request to fail job because the job is already failing. "
                                + "The ignored failure cause is",
                        e);
                return;
            }
            failed = true;

            schedulerExecutor.execute(() -> globalFailureHandler.handleGlobalFailure(e));
        }

        @Override
        public int currentParallelism() {
            return operatorParallelism;
        }

        @Override
        public ClassLoader getUserCodeClassloader() {
            return userCodeClassLoader;
        }

        @Override
        public CoordinatorStore getCoordinatorStore() {
            return coordinatorStore;
        }

        @Override
        public boolean isConcurrentExecutionAttemptsSupported() {
            return supportsConcurrentExecutionAttempts;
        }
    }
}
