package com.scaleunlimited.flinkkmeans;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinksources.UnionedSource;

/**
 * Cluster points via K-Means clustering. The algorithm is:
 *   - Start with N random clusters (centroids)
 *   - Randomly shuffle features (unassigned to centroids) to the ClusterFunction
 *      - Assign each feature to the nearest centroid
 *      - If the nearest centroid is different for that feature,
 *        remove the feature from the old centroid (output special centroid),
 *        add the feature to the new centroid (output special centroid),
 *        and reset the feature's "stable centroid" counter.
 *   - Broadcast centroids to every ClusterFunction
 *      - If it's a "regular" centroid, just record it.
 *      - If it's special "feature added" or "feature removed" centroid, update
 *        the centroid.
 *
 * Features are iterated on until their stable centroid counter reaches a target,
 * and which time they get emitted, along with the centroid they belong to.
 *
 */
public class KMeansClustering {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansClustering.class);

    public static void build(StreamExecutionEnvironment env, 
            SourceFunction<Centroid> centroidsSource, SourceFunction<Feature> featuresSource,
            SinkFunction<CentroidFeature> sink) {
        
        TypeInformation<Tuple2<Centroid, Feature>> type = TypeInformation.of(new TypeHint<Tuple2<Centroid, Feature>>(){});
        UnionedSource<Centroid, Feature> source = new UnionedSource<>(type, centroidsSource, featuresSource);

        DataStream<CentroidFeature> primer = env.addSource(source)
                .map(new Tuple2ToCentroidFeature());
        
        
        
        // Now comes some funky stuff. We want to broadcast centroids, but shuffle features. So we
        // have to split the stream, apply the appropriate distribution, and the re-combine.
        IterativeStream<Centroid> centroids = primer.split(new KmeansSelector())
                .select(KmeansSelector.CENTROID.get(0))
                .map(new ExtractCentroid())
                .broadcast()
                .iterate(5000L);
        
        IterativeStream<Feature> features = primer.split(new KmeansSelector())
                .select(KmeansSelector.FEATURE.get(0))
                .map(new ExtractFeature())
                .shuffle()
                .iterate(5000L);
        
        DataStream<CentroidFeature> clustered = centroids.connect(features)
            .flatMap(new ClusterFunction())
            .name("ClusterFunction");
        
        // We have to iterate on features and updates to centroids.
         iter.closeWith(clustered);
        
        // Output resulting features (with each one's centroid)
        clustered.split(new KmeansSelector())
            .select(KmeansSelector.RESULT.get(0))
            .addSink(sink);
    }
    
    @SuppressWarnings("serial")
    private static class Tuple2ToCentroidFeature implements MapFunction<Tuple2<Centroid, Feature>, CentroidFeature> {

        @Override
        public CentroidFeature map(Tuple2<Centroid, Feature> value) throws Exception {
            return new CentroidFeature(value.f0, value.f1);
        }
        
    }
    
    @SuppressWarnings("serial")
    private static class ExtractCentroid implements MapFunction<CentroidFeature, Centroid> {

        @Override
        public Centroid map(CentroidFeature value) throws Exception {
            if (value.getCentroid() == null) {
                LOGGER.warn("Got null centroid for {} ({})", value, System.identityHashCode(value));
            }
            
            return value.getCentroid();
        }
    }
    
    @SuppressWarnings("serial")
    private static class ExtractFeature implements MapFunction<CentroidFeature, Feature> {

        @Override
        public Feature map(CentroidFeature value) throws Exception {
            if (value.getFeature() == null) {
                LOGGER.warn("Got null feature for {})", value);
            }
            
            return value.getFeature();
        }
    }
    
    @SuppressWarnings("serial")
    private static class KmeansSelector implements OutputSelector<CentroidFeature> {

        public static final List<String> FEATURE = Arrays.asList("feature");
        public static final List<String> CENTROID = Arrays.asList("centroid");
        public static final List<String> RESULT = Arrays.asList("result");

        @Override
        public Iterable<String> select(CentroidFeature value) {
            if (value.getCentroid() != null) {
                if (value.getFeature() != null) {
                    LOGGER.debug("Returning RESULT for {}", value);
                    return RESULT;
                } else {
                    LOGGER.debug("Returning CENTROID for {}", value);
                    return CENTROID;
                }
            } else if (value.getFeature() != null) {
                LOGGER.debug("Returning FEATURE for {}", value);
                return FEATURE;
            } else {
                throw new RuntimeException("Invalid case of all fields null");
            }
        }
    }
    
    @SuppressWarnings("serial")
    private static class ClusterFunction extends RichCoFlatMapFunction<Centroid, Feature, CentroidFeature> {

        private transient Map<Integer, Centroid> centroids;
        protected transient int _parallelism;
        protected transient int _partition;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            RuntimeContext context = getRuntimeContext();
            _parallelism = context.getNumberOfParallelSubtasks();
            _partition = context.getIndexOfThisSubtask() + 1;

            centroids = new HashMap<>();
        }

        @Override
        public void flatMap1(Centroid centroid, Collector<CentroidFeature> out) {
            if (centroid == null) {
                LOGGER.warn("Null centroid >> Processing ({}/{})", _partition, _parallelism);
                return;
            }
            
            LOGGER.debug("{} >> Processing ({}/{})", centroid, _partition, _parallelism);
            
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
                        // We got an add for a cluster that we haven't received yet (could come from another operator),
                        // so send it around again.
                        out.collect(new CentroidFeature(centroid));
                        return;
                    }
                        
                    centroids.get(id).addFeature(centroid.getFeature());
                    break;
                
                case REMOVE:
                    if (!centroids.containsKey(id)) {
                        // We got a remove for a cluster that we haven't received yet (could come from another operator),
                        // so send it around again.
                        out.collect(new CentroidFeature(centroid));
                        return;
                    }
                    
                    centroids.get(id).removeFeature(centroid.getFeature());
                    break;
                
                default:
                    throw new RuntimeException(String.format("Got unknown centroid type %s!", centroid.getType()));
            }
        }

        @Override
        public void flatMap2(Feature feature, Collector<CentroidFeature> out) {
            if (feature == null) {
                LOGGER.warn("Null feature >> Processing ({}/{})", _partition, _parallelism);
                return;
            }
            
            LOGGER.debug("{} >> Processing ({}/{})", feature, _partition, _parallelism);

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
                throw new RuntimeException("Got feature without any centroids!");
            }

            int oldCentroidId = feature.getCentroidId();
            int newCentroidId = bestCentroid.getId();
            
            if (oldCentroidId == newCentroidId) {
                if (feature.getProcessCount() >= 5) {
                    LOGGER.debug("{} >> stable, removing", feature);
                    out.collect(new CentroidFeature(bestCentroid, feature));
                } else {
                    LOGGER.debug("{} >> not stable enough, recycling", feature);
                    Feature updatedFeature = new Feature(feature);
                    updatedFeature.incProcessCount();
                    out.collect(new CentroidFeature(updatedFeature));
                }
            } else {
                Feature updatedFeature = new Feature(feature);
                updatedFeature.setProcessCount(1);
                
                if (oldCentroidId != -1) {
                    Centroid removal = new Centroid(feature.times(feature.getProcessCount()), oldCentroidId, CentroidType.REMOVE);
                    out.collect(new CentroidFeature(removal));
                }

                LOGGER.debug("{} >> moving to {}", updatedFeature, bestCentroid.getId());
                updatedFeature.setCentroidId(newCentroidId);
                out.collect(new CentroidFeature(updatedFeature));
            }
            
            // We'll add the feature to the cluster, to continue influencing its
            // centroid.
            Centroid add = new Centroid(feature, newCentroidId, CentroidType.ADD);
            out.collect(new CentroidFeature(add));
        }
    }
}
