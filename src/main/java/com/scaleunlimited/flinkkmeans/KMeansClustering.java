package com.scaleunlimited.flinkkmeans;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster points via K-Means clustering. The algorithm is:
 *   - Start with N random clusters (centroids)
 *   - Feed points (unassigned to cluster) to the ClusterFunction
 *      - Assign each point to the nearest cluster
 *      - If the nearest cluster is different, iterate on the point
 *      - Recalculate cluster centroids
 *      - If a cluster centroid changes, iterate on the cluster (broadcast)
 *
 * We'll iterate until no points are changing clusters.
 *
 */
public class KMeansClustering {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansClustering.class);

    public static void build(StreamExecutionEnvironment env, 
            DataStream<Centroid> centroidsSource, DataStream<Feature> featuresSource,
            SinkFunction<Feature> sink) {
        IterativeStream<Centroid> centroidsIter = centroidsSource.iterate(15000L);
        IterativeStream<Feature> featuresIter = featuresSource.iterate(15000L);
        
        SplitStream<Tuple4<Feature,Centroid,Centroid,Feature>> comboStream = centroidsIter.broadcast()
            .connect(featuresIter.shuffle())
            .flatMap(new ClusterFunction())
            .name("ClusterFunction")
            .split(new KmeansSelector());
        
        centroidsIter.closeWith(comboStream.select(KmeansSelector.CENTROID_UPDATE.get(0))
                .map(new MapFunction<Tuple4<Feature,Centroid,Centroid,Feature>, Centroid>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Centroid map(Tuple4<Feature,Centroid,Centroid,Feature> value) throws Exception {
                        return value.f1;
                    }
                })
                .name("Centroid update"));
        
        featuresIter.closeWith(comboStream.select(KmeansSelector.FEATURE.get(0))
                .map(new MapFunction<Tuple4<Feature,Centroid,Centroid,Feature>, Feature>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Feature map(Tuple4<Feature,Centroid,Centroid,Feature> value) throws Exception {
                        return value.f0;
                    }
                })
                .name("Feature iteration"));
        
        comboStream.select(KmeansSelector.CENTROID_OUTPUT.get(0))
        .map(new MapFunction<Tuple4<Feature,Centroid,Centroid,Feature>, Centroid>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Centroid map(Tuple4<Feature,Centroid,Centroid,Feature> value) throws Exception {
                return value.f2;
            }
        })
        .name("Centroid output")
        .print();
        
        comboStream.select(KmeansSelector.FEATURE_OUTPUT.get(0))
        .map(new MapFunction<Tuple4<Feature,Centroid,Centroid,Feature>, Feature>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Feature map(Tuple4<Feature,Centroid,Centroid,Feature> value) throws Exception {
                return value.f3;
            }
        })
        .name("Feature output")
        .addSink(sink);
        // .name("Feature sink");
        
    }
    
    @SuppressWarnings("serial")
    private static class KmeansSelector implements OutputSelector<Tuple4<Feature, Centroid, Centroid, Feature>> {

        public static final List<String> FEATURE = Arrays.asList("feature");
        public static final List<String> FEATURE_OUTPUT = Arrays.asList("feature-output");
        public static final List<String> CENTROID_UPDATE = Arrays.asList("centroid-update");
        public static final List<String> CENTROID_OUTPUT = Arrays.asList("centroid-output");

        @Override
        public Iterable<String> select(Tuple4<Feature, Centroid, Centroid, Feature> value) {
            if (value.f0 != null) {
                return FEATURE;
            } else if (value.f1 != null) {
                return CENTROID_UPDATE;
            } else if (value.f2 != null) {
                return CENTROID_OUTPUT;
            } else {
                throw new RuntimeException("Invalid case of all fields null");
            }
        }
    }
    
    @SuppressWarnings("serial")
    private static class ClusterFunction extends RichCoFlatMapFunction<Centroid, Feature, Tuple4<Feature, Centroid, Centroid, Feature>> {

        private static int MAX_QEUEUED_FEATURES = 1000;
        
        private transient Map<Integer, Centroid> centroids;
        private transient Queue<Feature> featureQueue;
        private transient long centroidTimestamp;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            centroids = new HashMap<>();
            featureQueue = new LinkedList<>();
            centroidTimestamp = 0;
        }

        @Override
        public void flatMap1(Centroid centroid, Collector<Tuple4<Feature, Centroid, Centroid, Feature>> out) throws Exception {
            LOGGER.debug("Centroid {} @ {} >> Processing", centroid, centroid.getTime());
            
            centroidTimestamp = Math.max(centroidTimestamp, centroid.getTime());
            
            // Process queued features that are earlier than centroidTimestamp
            while (!featureQueue.isEmpty()) {
                Feature feature = featureQueue.peek();
                if (feature.getTime() <= centroidTimestamp) {
                    processFeature(featureQueue.remove(), out);
                }
            }
            
            int id = centroid.getId();
            switch (centroid.getType()) {
                case VALUE:
                    if (centroids.containsKey(id)) {
                        throw new RuntimeException(String.format("Got initial centroid %s but it already exists!", centroid));
                    }
                    
                    centroids.put(id, centroid);
                break;
                
                case ADD:
                    if (!centroids.containsKey(id)) {
                        throw new RuntimeException(String.format("Got centroid add %s but it doesn't exist!", centroid));
                    }
                
                    centroids.get(id).addFeature(centroid.getFeature());
                    break;
                
                case REMOVE:
                    if (!centroids.containsKey(id)) {
                        throw new RuntimeException(String.format("Got centroid remove %s but it doesn't exist!", centroid));
                    }
                
                    centroids.get(id).removeFeature(centroid.getFeature());
                    break;
                
                default:
                    throw new RuntimeException(String.format("Got unknown centroid type %s!", centroid.getType()));
                
            }
            
            out.collect(new Tuple4<>(null, null, centroids.get(id), null));
        }

        @Override
        public void flatMap2(Feature feature, Collector<Tuple4<Feature,Centroid,Centroid,Feature>> out) throws Exception {
            if (feature.getTime() > centroidTimestamp) {
                while (featureQueue.size() >= MAX_QEUEUED_FEATURES) {
                    processFeature(featureQueue.remove(), out);
                }
                
                LOGGER.debug("Feature {} @ {} >> queueing", feature, feature.getTime());
                featureQueue.add(feature);
                                
                return;
            } else {
                processFeature(feature, out);
            }
        }

        private void processFeature(Feature feature, Collector<Tuple4<Feature,Centroid,Centroid,Feature>> out) {
            double minDistance = Double.MAX_VALUE;
            Centroid bestCentroid = null;
            for (Centroid centroid : centroids.values()) {
                double distance = centroid.distance(feature);
                if (distance < minDistance) {
                    minDistance = distance;
                    bestCentroid = centroid;
                }
            }

            if (bestCentroid == null) {
                LOGGER.debug("Feature {} @ {} >> No centroids yet, recycling", feature, feature.getTime());
                feature.setTime(System.currentTimeMillis());
                out.collect(new Tuple4<>(feature, null, null, null));
                return;
            }

            int oldCentroidId = feature.getCentroidId();
            int newCentroidId = bestCentroid.getId();
            
            if (oldCentroidId == newCentroidId) {
                feature.incProcessCount();
                if (feature.getProcessCount() >= 20) {
                    LOGGER.debug("Feature {} @ {} >> stable, removing", feature, feature.getTime());
                    out.collect(new Tuple4<>(null, null, null, feature));
                } else {
                    LOGGER.debug("Feature {} @ {} >> not stable enough, recycling", feature, feature.getTime());
                    feature.setTime(System.currentTimeMillis());
                    out.collect(new Tuple4<>(feature, null, null, null));
                }
            } else {
                feature.setProcessCount(1);
                
                if (oldCentroidId != -1) {
                    Centroid removal = new Centroid(feature.times(feature.getProcessCount()), oldCentroidId, CentroidType.REMOVE);
                    out.collect(new Tuple4<>(null, removal, null, null));
                }

                LOGGER.debug("Feature {} @ {} >> moving to {}", feature, feature.getTime(), bestCentroid.getId());
                feature.setCentroidId(newCentroidId);
                feature.setTime(System.currentTimeMillis());
                out.collect(new Tuple4<>(feature, null, null, null));
            }
            
            // We'll add the value to the cluster, to continue influencing its
            // centroid.
            Centroid add = new Centroid(feature, newCentroidId, CentroidType.ADD);
            out.collect(new Tuple4<>(null, add, null, null));
        }

    }
}
