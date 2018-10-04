package com.scaleunlimited.flinkkmeans;

import java.util.concurrent.ExecutionException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;

public class KMeansMiniCluster {

    private static final int NUM_TMS = 2;
    private static final int NUM_SLOTS_PER_TM = 2;

    private static final int QS_PROXY_PORT_RANGE_START = 9084;
    private static final int QS_SERVER_PORT_RANGE_START = 9089;

    private QueryableStateClient client;
    private ClusterClient<?> clusterClient;
    private int maxParallelism = NUM_TMS * NUM_SLOTS_PER_TM;
    private MiniClusterResource MINI_CLUSTER_RESOURCE;
    
    private transient JobID jobID;
    private transient AutoCancellableJob autoCancellableJob;
    
    public KMeansMiniCluster() {
        MINI_CLUSTER_RESOURCE = new MiniClusterResource(
                new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(getConfig())
                .setNumberTaskManagers(NUM_TMS)
                .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
                .build());
    }
    
    public JobSubmissionResult start(Deadline deadline, StreamExecutionEnvironment env) throws Exception {
        MINI_CLUSTER_RESOURCE.before();
        
        client = new QueryableStateClient("localhost", QS_PROXY_PORT_RANGE_START);
        client.setExecutionConfig(new ExecutionConfig());
        clusterClient = MINI_CLUSTER_RESOURCE.getClusterClient();
        
        autoCancellableJob = new AutoCancellableJob(deadline, clusterClient, env);
        jobID = autoCancellableJob.getJobId();
        final JobGraph jobGraph = autoCancellableJob.getJobGraph();

        clusterClient.setDetached(true);
        return clusterClient.submitJob(jobGraph, KMeansMiniCluster.class.getClassLoader());
    }
    
    public QueryableStateClient getQueryableStateClient() {
        return client;
    }
    
    public void stop() throws Exception {
        client.shutdownAndWait();
        
        try {
            autoCancellableJob.close();
        } catch (Exception e) {
            throw e;
        } finally {
            clusterClient.shutDownCluster();
            MINI_CLUSTER_RESOURCE.after();
        }
    }
    
    public JobID getJobID() {
        return jobID;
    }
    
    public boolean isRunning() throws InterruptedException, ExecutionException {
        return autoCancellableJob.isRunning();
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public static Configuration getConfig() {
        Configuration config = new Configuration();
        config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "4m");
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
        config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);
        config.setString(
                QueryableStateOptions.PROXY_PORT_RANGE,
                QS_PROXY_PORT_RANGE_START + "-" + (QS_PROXY_PORT_RANGE_START + NUM_TMS));
            config.setString(
                QueryableStateOptions.SERVER_PORT_RANGE,
                QS_SERVER_PORT_RANGE_START + "-" + (QS_SERVER_PORT_RANGE_START + NUM_TMS));
        config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
        return config;
    }



}
