package com.scaleunlimited.flinkkmeans;

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

import com.scaleunlimited.flinksources.TimedTerminator;
import com.scaleunlimited.flinksources.UnionedSources;

/**
 * Cluster points via K-Means clustering. The algorithm is:
 *   - Start with N random clusters (centroids)
 *   - Randomly shuffle features to the ClusterFunction
 *      - Assign each feature to the nearest centroid
 *      - If the nearest centroid is different for that feature,
 *        remove the feature from the old centroid (output special centroid),
 *        add the feature to the new centroid (output special centroid),
 *        and reset the feature's "stable centroid" counter.
 *      - If the nearest centroid is the same as before, increment
 *        a counter. If the counter is < some "stable" limit, send
 *        the feature around again (iterate on it) via a side output,
 *        otherwise collect it - this then removes it from the iteration.
 *   - Broadcast centroids to every ClusterFunction
 *      - If it's a "regular" centroid, just record it.
 *      - If it's special "feature added" or "feature removed" centroid, update
 *        the corresponding centroid.
 *
 * Features are iterated on until their stable centroid counter reaches a target,
 * and which time they get emitted, along with the centroid they belong to. Note
 * that centroids can and will move around, thus the location of centroid id X
 * won't be constant in the output stream.
 *
 */
public class KMeansClustering {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansClustering.class);

    private static final String CENTROID_TAG = "centroid";
    private static final List<String> CENTROID_SELECTOR = Collections.singletonList(CENTROID_TAG);
    
    private static final String FEATURE_TAG = "feature";
    private static final List<String> FEATURE_SELECTOR = Collections.singletonList(FEATURE_TAG);
    
    public static void build(StreamExecutionEnvironment env, 
            SourceFunction<Centroid> centroidsSource, SourceFunction<Feature> featuresSource,
            SinkFunction<FeatureResult> sink) {
        
        TypeInformation<Either<Centroid, Feature>> type = TypeInformation.of(new TypeHint<Either<Centroid, Feature>>(){});
        UnionedSources<Centroid, Feature> source = new UnionedSources<>(type, centroidsSource, featuresSource);
        
        // TODO - pass in terminator to the build() method, as the test code knows what it wants.
        source.setTerminator(new TimedTerminator(5000L));
        
        // Split the stream into centroids and features, then set up separate iterations for
        // each one. Centroids are broadcast, while features are shuffled.
        SplitStream<Either<Centroid, Feature>> primer = env.addSource(source)
                .name("centroids and features")
                .split(either -> either.isLeft() ? CENTROID_SELECTOR : FEATURE_SELECTOR);
        
        IterativeStream<Centroid> centroids = primer.select(CENTROID_TAG)
                .map(either -> either.left())
                .name("centroids")
                .returns(Centroid.class)
                .iterate(5000L);
        
        IterativeStream<Feature> features = primer.select(FEATURE_TAG)
                .map(either -> either.right())
                .name("features")
                .returns(Feature.class)
                .iterate(5000L);
        
        SingleOutputStreamOperator<FeatureResult> clustered = centroids
                .broadcast()
                .connect(features.shuffle())
                .process(new ClusterFunction())
                .name("ClusterFunction");
        
        // We have to iterate on features, and centroid updates.
        features.closeWith(clustered.getSideOutput(ClusterFunction.FEATURE_OUTPUT_TAG));
        centroids.closeWith(clustered.getSideOutput(ClusterFunction.CENTROID_OUTPUT_TAG));
        
        // Output resulting features (with each one's current centroid)
        clustered.addSink(sink)
            .name("results");
        
        // Also make the resulting clusters queryable...
        clustered.map(result -> result.getCentroid())
            .keyBy(centroid -> centroid.getId())
            .asQueryableState("centroids");
    }
    
    @SuppressWarnings("serial")
    private static class ClusterFunction extends CoProcessFunction<Centroid, Feature, FeatureResult> {

        public static final OutputTag<Centroid> CENTROID_OUTPUT_TAG = new OutputTag<Centroid>("centroid"){};
        public static final OutputTag<Feature> FEATURE_OUTPUT_TAG = new OutputTag<Feature>("feature"){};

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
        public void processElement1(Centroid centroid, Context ctx, Collector<FeatureResult> out) throws Exception {
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
                    
                    centroids.put(id, new Centroid(centroid));
                break;
                
                case ADD:
                    if (!centroids.containsKey(id)) {
                        // We got an add for a cluster that we haven't received yet (could come from another operator),
                        // so send it around again.
                        ctx.output(CENTROID_OUTPUT_TAG, new Centroid(centroid));
                    } else {
                        centroids.get(id).addFeature(centroid.getFeature());
                    }
                    
                    break;
                
                case REMOVE:
                    if (!centroids.containsKey(id)) {
                        // We got a remove for a cluster that we haven't received yet (could come from another operator),
                        // so send it around again.
                        ctx.output(CENTROID_OUTPUT_TAG, new Centroid(centroid));
                    } else {
                        centroids.get(id).removeFeature(centroid.getFeature());
                    }
                    
                    break;
                
                default:
                    throw new RuntimeException(String.format("Got unknown centroid type %s!", centroid.getType()));
            }
        }

        @Override
        public void processElement2(Feature feature, Context ctx, Collector<FeatureResult> out) throws Exception {
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
                    out.collect(new FeatureResult(feature, bestCentroid));
                } else {
                    LOGGER.debug("{} >> not stable enough, recycling", feature);
                    Feature updatedFeature = new Feature(feature);
                    updatedFeature.incProcessCount();
                    ctx.output(FEATURE_OUTPUT_TAG, new Feature(updatedFeature));
                }
            } else {
                Feature updatedFeature = new Feature(feature);
                updatedFeature.setProcessCount(1);
                
                if (oldCentroidId != -1) {
                    Centroid removal = new Centroid(feature.times(feature.getProcessCount()), oldCentroidId, CentroidType.REMOVE);
                    ctx.output(CENTROID_OUTPUT_TAG, new Centroid(removal));
                }

                LOGGER.debug("{} >> moving to {}", updatedFeature, bestCentroid.getId());
                updatedFeature.setCentroidId(newCentroidId);
                ctx.output(FEATURE_OUTPUT_TAG, new Feature(updatedFeature));
            }
            
            // We'll add the feature to the cluster, to continue influencing its
            // centroid.
            Centroid add = new Centroid(feature, newCentroidId, CentroidType.ADD);
            ctx.output(CENTROID_OUTPUT_TAG, new Centroid(add));
        }

    }
}
