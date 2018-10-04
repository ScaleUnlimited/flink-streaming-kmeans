package com.scaleunlimited.flinkkmeans;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinksources.SourceTerminator;
import com.scaleunlimited.flinksources.TimedTerminator;
import com.scaleunlimited.flinksources.UnionedSources;

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
        
        SingleOutputStreamOperator<FeatureResult> clustered = clusters
                .broadcast()
                .connect(features.shuffle())
                .process(new ClusterFunction(seedClusters, clusterDistance))
                .name("ClusterFunction");
        
        // We have to iterate on features, and cluster updates.
        features.closeWith(clustered.getSideOutput(ClusterFunction.FEATURE_OUTPUT_TAG));
        clusters.closeWith(clustered.getSideOutput(ClusterFunction.CLUSTER_UPDATE_OUTPUT_TAG));
        
        // Output resulting features (with each one's current centroid)
        clustered.addSink(sink)
            .name("results");
        
        // Also make the resulting clusters queryable. Note cheesy hack where the centroid
        // feature for the cluster has its ID set to the cluster id.
        clustered.map(result -> result.getCentroid())
            .keyBy(centroid -> centroid.getId())
            .asQueryableState("centroids");
    }
    
    @SuppressWarnings("serial")
    private static class ClusterFunction extends CoProcessFunction<ClusterUpdate, Feature, FeatureResult> {

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
        }

        @Override
        public void processElement1(ClusterUpdate clusterUpdate, Context ctx, Collector<FeatureResult> out) throws Exception {
            LOGGER.debug("{} >> Processing cluster update ({}/{})", clusterUpdate, _partition, _parallelism);
            
            // Cluster update has a feature, moving from one cluster (might be no cluster, for new feature) to another
            // cluster (might be no cluster, for a stable/emitted feature).
            int fromCluster = clusterUpdate.getFromClusterId();
            int toCluster = clusterUpdate.getToClusterId();
            
            if (fromCluster != Cluster.NO_CLUSTER_ID) {
                Cluster c = clusters.get(fromCluster);
                c.removeFeature(clusterUpdate.getFeature());
            }
            
            if (toCluster != Cluster.NO_CLUSTER_ID) {
                Cluster c = clusters.get(toCluster);
                c.addFeature(clusterUpdate.getFeature());
            }
        }

        @Override
        public void processElement2(Feature feature, Context ctx, Collector<FeatureResult> out) throws Exception {
            LOGGER.debug("{} >> Processing feature ({}/{})", feature, _partition, _parallelism);

            double minDistance = Double.MAX_VALUE;
            double curDistance = Double.MAX_VALUE;
            Cluster bestCluster = null;
            Cluster unusedCluster = null;
            for (Cluster cluster : clusters.values()) {
                double distance = cluster.distance(feature);
                
                if (feature.getClusterId() == cluster.getId()) {
                    curDistance = distance;
                }
                
                if ((distance == Cluster.UNUSED_DISTANCE) && isMyCluster(cluster)) {
                    unusedCluster = cluster;
                }

                if (distance <= minDistance) {
                    minDistance = distance;
                    bestCluster = cluster;
                }
            }

            // Special case handling of when the feature is far away from the best
            // (closest) cluster, and we have an unused cluster that we can immediately
            // use.
            if ((minDistance > _newClusterDistance) && (unusedCluster != null)) {
                LOGGER.debug("{} >> initial cluster  for {}", feature, unusedCluster.getId());

                unusedCluster.addFeature(feature);
                
                feature.setProcessCount(1);
                Feature updatedFeature = new Feature(feature);
                updatedFeature.setClusterId(unusedCluster.getId());
                ctx.output(FEATURE_OUTPUT_TAG, updatedFeature);
                return;
            }
            
            int oldClusterId = feature.getClusterId();
            int newClusterId = bestCluster.getId();
            
            if ((minDistance > 1000) && (minDistance != Double.MAX_VALUE)) {
                LOGGER.debug("{} >> far from best cluster {}", feature, bestCluster);
            }
            
            if (oldClusterId == newClusterId) {
                // TODO make stable count configurable, vary by inflight count.
                if (feature.getProcessCount() >= 5) {
                    LOGGER.debug("{} >> stable, removing", feature);
                    out.collect(new FeatureResult(feature, bestCluster));
                    
                    // So that old features stop influencing the cluster centroid.
                    ctx.output(CLUSTER_UPDATE_OUTPUT_TAG, new ClusterUpdate(feature, oldClusterId, Cluster.NO_CLUSTER_ID));
                } else {
                    LOGGER.debug("{} >> not stable enough, recycling", feature);
                    Feature updatedFeature = new Feature(feature);
                    updatedFeature.incProcessCount();
                    ctx.output(FEATURE_OUTPUT_TAG, updatedFeature);
                }
            } else {
                if (oldClusterId != Cluster.NO_CLUSTER_ID) {
                    LOGGER.debug("{} >> moving to {} (cur distance {}, new distance {}", feature,
                            newClusterId, curDistance, minDistance);
                } else {
                    LOGGER.debug("{} >> assigning to {}", feature, newClusterId);
                }
                
                feature.setProcessCount(1);
                ctx.output(CLUSTER_UPDATE_OUTPUT_TAG, new ClusterUpdate(feature, oldClusterId, newClusterId));

                Feature updatedFeature = new Feature(feature);
                updatedFeature.setClusterId(newClusterId);
                ctx.output(FEATURE_OUTPUT_TAG, updatedFeature);
            }
        }

        private boolean isMyCluster(Cluster cluster) {
            // This cluster "belongs" to this operator if its ID belongs
            // to this operator's partition. The _partition number is
            // base 1...
            return (cluster.getId() % _parallelism) == (_partition - 1);
        }

    }
}
