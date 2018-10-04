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

package com.scaleunlimited.flinkkmeans;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

/**
 * A wrapper of the job graph that makes sure to cancel the job and wait for
 * termination after the execution.
 */
public class AutoCancellableJob implements AutoCloseable {

    private final ClusterClient<?> clusterClient;
    private final JobGraph jobGraph;
    private final JobID jobId;
    private final Deadline deadline;

    AutoCancellableJob(Deadline deadline, final ClusterClient<?> clusterClient, final StreamExecutionEnvironment env) {
        Preconditions.checkNotNull(env);

        this.clusterClient = Preconditions.checkNotNull(clusterClient);
        this.jobGraph = env.getStreamGraph().getJobGraph();
        this.jobId = Preconditions.checkNotNull(jobGraph.getJobID());

        this.deadline = deadline;
    }

    JobGraph getJobGraph() {
        return jobGraph;
    }

    JobID getJobId() {
        return jobId;
    }

    public boolean isRunning() throws InterruptedException, ExecutionException {
        CompletableFuture<JobStatus> jobStatusFuture = clusterClient.getJobStatus(jobId);
        JobStatus status = jobStatusFuture.get();
        return status == JobStatus.RUNNING;
    }
    
    @Override
    public void close() throws Exception {
        // Free cluster resources
        clusterClient.cancel(jobId);
        // cancel() is non-blocking so do this to make sure the job finished
        CompletableFuture<JobStatus> jobStatusFuture = FutureUtils.retrySuccesfulWithDelay(
            () -> clusterClient.getJobStatus(jobId),
            Time.milliseconds(50),
            deadline,
            (jobStatus) -> jobStatus.equals(JobStatus.CANCELED),
            TestingUtils.defaultScheduledExecutor());
        assertEquals(
            JobStatus.CANCELED,
            jobStatusFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));
    }
}
