package com.scaleunlimited.flinkkmeans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster points via K-Means clustering. The algorithm is:
 *   - Start with N random clusters, or create new clusters (up to a limit)
 *      if a feature is too far from the closest cluster 
 *   - Randomly shuffle features to the ClusterFunction
 *      - Assign each feature to the nearest cluster
 *      - If the nearest cluster is different for that feature,
 *        remove the feature from the old cluster, add the feature to 
 *        the new cluster, and reset the feature's "stable cluster" counter.
 *      - If the nearest cluster is the same as before, increment
 *        a counter. If the counter is < some "stable" limit, send
 *        the feature around again (iterate on it) via a side output,
 *        otherwise collect it - this then removes it from the iteration.
 *   - Broadcast cluster updates to every ClusterFunction
 *
 * Features are iterated on until their stable cluster counter reaches a target,
 * and which time they get emitted, along with the centroid they belong to. Note
 * that cluster centroids can and will move around, thus the location (centroid) of
 * clusters won't be constant in the output stream.
 *
 */
public class KMeansClustering {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansClustering.class);

    private static final MapStateDescriptor<Integer, Cluster> CLUSTER_STATE_DESCRIPTOR = new MapStateDescriptor<Integer, Cluster>(
                "clusters", Integer.class, Cluster.class);

    public static void build(StreamExecutionEnvironment env, SourceFunction<Feature> featuresSource,
            SinkFunction<FeatureResult> sink, int numClusters, double clusterDistance) {
        
        // Create unused clusters.
        List<Cluster> clusters = new ArrayList<>();
        for (int i = 0; i < numClusters; i++) {
            clusters.add(i, new Cluster(i));
        }

        build(env, featuresSource, sink, clusters, clusterDistance);
    }
    
    public static void build(StreamExecutionEnvironment env, SourceFunction<Feature> featuresSource,
            SinkFunction<FeatureResult> sink, List<Cluster> seedClusters, double clusterDistance) {
        
        // HACK! We have to provide a source that runs forever (or until the timer terminates it),
        // so that checkpointing works with iterations.
        IterativeStream<ClusterUpdate> clusters = env.addSource(new EmptyTimedSource<ClusterUpdate>())
                .returns(ClusterUpdate.class)
                .name("clusters")
                .iterate(5000L);
        
        IterativeStream<Feature> features = env.addSource(featuresSource)
                .returns(Feature.class)
                .name("features")
                .iterate(5000L);
        
        SingleOutputStreamOperator<FeatureResult> clustered = features
                .keyBy(feature -> feature.getId())
                .connect(clusters.broadcast(CLUSTER_STATE_DESCRIPTOR))
                .process(new ClusterFunction(seedClusters, clusterDistance))
                .name("ClusterFunction");
        
        // We have to iterate on features, and cluster updates.
        features.closeWith(clustered.getSideOutput(ClusterFunction.FEATURE_OUTPUT_TAG));
        clusters.closeWith(clustered.getSideOutput(ClusterFunction.CLUSTER_UPDATE_OUTPUT_TAG));
        
        // Make the results queryable (both clusters and features),
        // and output to the sink.
        clustered.keyBy(result -> result.getClusterId())
            .map(new QueryableFeatureResult())
            .addSink(sink)
            .name("results");
    }
    
    @SuppressWarnings("serial")
    private static class QueryableFeatureResult extends RichMapFunction<FeatureResult, FeatureResult> implements CheckpointedFunction {

        private transient ValueState<FeatureResult> _clusterResults;

        @Override
        public FeatureResult map(FeatureResult result) throws Exception {
            // TODO Auto-generated method stub
            return result;
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            ValueStateDescriptor<FeatureResult> descriptor =
                    new ValueStateDescriptor<>(
                            "feature-results", // the state name
                            TypeInformation.of(new TypeHint<FeatureResult>() {}));
            descriptor.setQueryable("centroids");
            
            _clusterResults = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            // Nothing to do, managed for us.
        }
    }
    
    
    @SuppressWarnings("serial")
    private static class ClusterFunction extends KeyedBroadcastProcessFunction<Integer, Feature, ClusterUpdate, FeatureResult> {

        public static final OutputTag<ClusterUpdate> CLUSTER_UPDATE_OUTPUT_TAG = new OutputTag<ClusterUpdate>("cluster-update"){};
        public static final OutputTag<Feature> FEATURE_OUTPUT_TAG = new OutputTag<Feature>("feature"){};

        private double _newClusterDistance;
        private Map<Integer, Cluster> clusters;
        
        protected transient int _parallelism;
        protected transient int _partition;

        public ClusterFunction(List<Cluster> seedClusters, double newClusterDistance) {
            _newClusterDistance = newClusterDistance;
            
            clusters = new HashMap<>();
            for (Cluster cluster : seedClusters) {
                clusters.put(cluster.getId(), cluster);
            }
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            RuntimeContext context = getRuntimeContext();
            _parallelism = context.getNumberOfParallelSubtasks();
            _partition = context.getIndexOfThisSubtask() + 1;
            
            // Figure out whether we have a valid starting set of clusters.
            int numUsed = 0;
            int numUnused = 0;
            for (Cluster c : clusters.values()) {
                if (c.isUnused()) {
                    numUnused++;
                } else {
                    numUsed++;
                }
            }
            
            if (numUsed + numUnused == 0) {
                throw new IllegalStateException("Must have at least one seed cluster");
            } else if ((numUsed != 0) && (numUnused != 0)) {
                throw new IllegalStateException("Seed clusters must be either all used or all unused");
            } else if ((numUnused > 0) && (numUnused < _parallelism)) {
                // We need to have at least as many clusters as we have operators, so that we
                // can put a feature into an unused cluster that our operator instance "owns".
                throw new IllegalStateException("Number of unused seed clusters must be >= parallelism");
            }
        }

        private boolean isMyCluster(int clusterId) {
            // This cluster "belongs" to this operator if its ID belongs
            // to this operator's partition. The _partition number is
            // base 1...
            return (clusterId % _parallelism) == (_partition - 1);
        }

        @Override
        public void processBroadcastElement(ClusterUpdate clusterUpdate, Context ctx, Collector<FeatureResult> out) throws Exception {
            LOGGER.debug("{} >> Processing cluster update ({}/{})", clusterUpdate, _partition, _parallelism);

            // TODO - use BroadcastState, not our own map, to keep track of clusters.
            // ctx.getBroadcastState(CLUSTER_STATE_DESCRIPTOR)
            
            // Cluster update has a feature, moving from one cluster (might be no cluster, for new feature) to another
            // cluster (might be no cluster, for a stable/emitted feature).
            int fromCluster = clusterUpdate.getFromClusterId();
            int toCluster = clusterUpdate.getToClusterId();
            Feature feature = clusterUpdate.getFeature();

            if (fromCluster != Cluster.NO_CLUSTER_ID) {
                Cluster c = clusters.get(fromCluster);
                c.removeFeature(feature);
            }

            if (toCluster != Cluster.NO_CLUSTER_ID) {
                if (isMyCluster(toCluster) && clusterUpdate.isNewCluster()) {
                    // Ignore an update to a cluster that we did an immediate
                    // update for in processElement2
                } else {
                    Cluster c = clusters.get(toCluster);
                    c.addFeature(feature);
                }
            }
        }

        @Override
        public void processElement(Feature feature, ReadOnlyContext ctx, Collector<FeatureResult> out) throws Exception {
            LOGGER.debug("{} >> Processing feature ({}/{})", feature, _partition, _parallelism);

            // TODO - use BroadcastState, not our own map, to find the best cluster.

            // Find the best (closest) cluster. We assume we have at least as many clusters as
            // we have parallel operators, as otherwise we won't be able to find one of "our"
            // clusters that's unused, when we first start out with all unused clusters.
            double minDistance = Double.MAX_VALUE;
            Cluster bestCluster = null;
            Cluster unusedCluster = null;
            for (Cluster cluster : clusters.values()) {
                if (cluster.isUnused()) {
                    if (isMyCluster(cluster.getId())) {
                        unusedCluster = cluster;
                    } else {
                        // Ignore unused clusters that don't "belong" to our
                        // operator instance, as these will be assigned by
                        // other operator instances.
                    }
                } else {
                    double distance = cluster.distance(feature);
                    if (distance <= minDistance) {
                        minDistance = distance;
                        bestCluster = cluster;
                    }
                }
            }

            // Special case handling of when the feature is far away from the best
            // (closest) cluster, and we have an unused cluster that we can immediately
            // use because it's "our" cluster. We want to do this immediate update so
            // that we don't get a bunch of features all trying to grab the same unused
            // cluster before it gets updated via the ClusterUpdate iteration.
            boolean isNewCluster = false;
            if ((minDistance > _newClusterDistance) && (unusedCluster != null)) {
                LOGGER.debug("{} >> setting {} as initial cluster", feature, unusedCluster.getId());

                isNewCluster = true;
                bestCluster = unusedCluster;
                bestCluster.addFeature(feature);
            }
            
            int oldClusterId = feature.getClusterId();
            int newClusterId = bestCluster.getId();
            
            if (oldClusterId == newClusterId) {
                // Increment the feature's stable count, and see if it's high enough
                // to emit the feature
                // TODO make stable count configurable, vary by in-flight count.
                if (feature.getProcessCount() >= 5) {
                    LOGGER.debug("{} >> stable, removing", feature);
                    out.collect(new FeatureResult(feature, clusters.get(oldClusterId)));
                    
                    // So that old features stop influencing the cluster centroid, we
                    // want to remove from the old cluster, but not put it into a new cluster.
                    newClusterId = Cluster.NO_CLUSTER_ID;
                } else {
                    LOGGER.debug("{} >> not stable enough, recycling", feature);
                    Feature updatedFeature = new Feature(feature);
                    updatedFeature.incProcessCount();
                    ctx.output(FEATURE_OUTPUT_TAG, updatedFeature);
                }
            } else {
                Feature updatedFeature = new Feature(feature);
                updatedFeature.setClusterId(newClusterId);
                updatedFeature.setProcessCount(1);
                ctx.output(FEATURE_OUTPUT_TAG, updatedFeature);
            }
            
            if (oldClusterId != newClusterId) {
                ClusterUpdate clusterUpdate = new ClusterUpdate(feature, oldClusterId, newClusterId, isNewCluster);
                ctx.output(CLUSTER_UPDATE_OUTPUT_TAG, clusterUpdate);
            }
        }

    }
}
