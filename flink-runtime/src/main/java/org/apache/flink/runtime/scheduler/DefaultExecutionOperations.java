/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.Execution;

import java.util.concurrent.CompletableFuture;

/** Default implementation of {@link ExecutionOperations}. */
public class DefaultExecutionOperations implements ExecutionOperations {

    @Override
    public void deploy(Execution execution) throws JobException {
        // execution包含了这个Task运行时所需要的各种信息 上游  下游 operator中的UserFunction 在最开始的时候，构造StreaGraph的时候，每个顶点都赋予了一个启动类，启动类TaskInvokable
        //jar包 依赖jar包在这个对象中吗 不是的，在blobServer里面
        //Task初始化和启动 14个动作 其中的一个动作 就是从BlobServer下载Job和Task的相关信息和jar等
        execution.deploy();
    }

    @Override
    public CompletableFuture<?> cancel(Execution execution) {
        execution.cancel();
        return execution.getReleaseFuture();
    }

    @Override
    public void markFailed(Execution execution, Throwable cause) {
        execution.markFailed(cause);
    }
}
