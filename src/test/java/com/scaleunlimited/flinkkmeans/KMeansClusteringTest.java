package com.scaleunlimited.flinkkmeans;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansClusteringTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansClusteringTest.class);

    @Test
    public void test() throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2);

        String[] centers = KMeansData.INITIAL_CENTERS_2D.split("\n");
        List<Centroid> centroids = new ArrayList<>();
        for (String c : centers) {
            String[] fields = c.split("\\|");
            Feature f = new Feature(Double.parseDouble(fields[1]),
                    Double.parseDouble(fields[2]));
            centroids.add(new Centroid(f, Integer.parseInt(fields[0]), CentroidType.VALUE));
        }
        
        SourceFunction<Centroid> centroidsSource = new ParallelListSource<Centroid>(centroids);
        
        String[] points = KMeansData.DATAPOINTS_2D.split("\n");
        List<Feature> features = new ArrayList<>();
        for (String p : points) {
            String[] fields = p.split("\\|");
            
            Feature f = new Feature(
                        Integer.parseInt(fields[0]),
                        Double.parseDouble(fields[1]),
                        Double.parseDouble(fields[2]));
            features.add(clusterize(centroids, f));
        }
        
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features);

        InMemorySinkFunction sink = new InMemorySinkFunction();
        KMeansClustering.build(env, centroidsSource, featuresSource, sink);
        
        FileUtils.write(new File("./target/kmeans-graph.dot"), FlinkUtils.planToDot(env.getExecutionPlan()));
        
        env.execute();
        
        Queue<Tuple2<Centroid, Feature>> results = InMemorySinkFunction.getValues();
        assertEquals(points.length, results.size());
        
        Map<Integer, Centroid> clusters = ClusterizePoints.createCentroids(points);
        
        while (!results.isEmpty()) {
            Tuple2<Centroid, Feature> result = results.remove();
            Centroid c = result.f0;
            Feature f = result.f1;
            LOGGER.info("Feature {} assigned to centroid {} at {},{}", f, c.getId(), f.getX(), f.getY());
            
            if (f.getCentroidId() != f.getTargetCentroidId()) {
                double actualDistance = f.distance(clusters.get(f.getCentroidId()).getFeature());
                double targetDistance = f.distance(clusters.get(f.getTargetCentroidId()).getFeature());
                if (actualDistance > targetDistance) {
                    fail(String.format("Got %d (%f), expected %d (%f) for %s\n", 
                            f.getCentroidId(), actualDistance, 
                            f.getTargetCentroidId(), targetDistance,
                            f));
                }
            }
        }
    }

    private Feature clusterize(List<Centroid> centroids, Feature value) throws Exception {
        double minDistance = Double.MAX_VALUE;
        Centroid bestCentroid = null;
        for (Centroid centroid : centroids) {
            double distance = centroid.distance(value);
            if (distance < minDistance) {
                minDistance = distance;
                bestCentroid = centroid;
            }
        }

        // Move the point part of the way to the best centroid cluster
        double newX = value.getX() - (value.getX() - bestCentroid.getFeature().getX()) * 0.3;
        double newY = value.getY() - (value.getY() - bestCentroid.getFeature().getY()) * 0.3;
        
//        LOGGER.debug(String.format("Moving towards cluster %d (%f,%f): %f,%f => %f,%f", 
//                bestCentroid.getId(), bestCentroid.getFeature().getX(), bestCentroid.getFeature().getY(),
//                value.getX(), value.getY(), newX, newY));
        return new Feature(value.getId(), newX, newY, -1, bestCentroid.getId());
    }

    @Ignore
    @Test
    public void testSyntheticData() throws Exception {
        final StreamExecutionEnvironment env = 
                StreamExecutionEnvironment.createLocalEnvironment(2);

        final int numCentroids = 2;
        Centroid[] centroids = makeCentroids(numCentroids);
        DataStream<Centroid> centroidsSource = env.fromElements(Centroid.class, centroids);

        final int numPoints = numCentroids * 100;
        DataStream<Feature> featuresSource = env.fromElements(Feature.class,
                makeFeatures(centroids, numPoints));

        InMemorySinkFunction sink = new InMemorySinkFunction();
        // KMeansClustering.build(env, centroidsSource, featuresSource, sink);

        env.execute();
    }

    private static Centroid[] makeCentroids(int numCentroids) {
        Centroid[] result = new Centroid[numCentroids];
        for (int i = 0; i < numCentroids; i++) {
            int x = ((i + 1) * 10);
            int y = ((i + 1) * 20);
            result[i] = new Centroid(new Feature(x, y), i, CentroidType.VALUE);
        }

        return result;
    }

    private static Feature[] makeFeatures(Centroid[] centroids, int numFeatures) {
        int numCentroids = centroids.length;
        Random rand = new Random(0L);
        Feature[] result = new Feature[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            double x = rand.nextDouble() * 10 * numCentroids;
            double y = rand.nextDouble() * 20 * numCentroids;
            result[i] = new Feature(x, y);
        }

        return result;
    }

    @SuppressWarnings("serial")
    private static class InMemorySinkFunction extends RichSinkFunction<Tuple2<Centroid, Feature>> {
        
        // Static, so all parallel functions will write to the same queue
        static private Queue<Tuple2<Centroid, Feature>> _values = new ConcurrentLinkedQueue<>();
        
        public static Queue<Tuple2<Centroid, Feature>> getValues() {
            return _values;
        }
        
        @Override
        public void invoke(Tuple2<Centroid, Feature> value) throws Exception {
            LOGGER.debug("Adding feature {} for centroid {}", value.f1, value.f0);
            _values.add(value);
        }
    }

    /**
     * Given a set of target centroids, "move" each feature towards the closest
     * centroid (effectively creating more clustered data), and record in the
     * feature the centroid it moved towards, for test validation purposes.
     *
     */
    @SuppressWarnings("serial")
    private static class ClusterizePoints extends RichMapFunction<Feature, Feature> {
        
        private String[] clusters;
        private transient Map<Integer, Centroid> centroids;
        
        public ClusterizePoints(String[] clusters) {
            this.clusters = clusters;
        }

        public static Map<Integer, Centroid> createCentroids(String[] points) {
            Map<Integer, Centroid> result = new HashMap<>();
            
            for (String p : points) {
                String[] fields = p.split("\\|");
                Feature f = new Feature(Double.parseDouble(fields[1]),
                        Double.parseDouble(fields[2]));
                Centroid c = new Centroid(f, Integer.parseInt(fields[0]), CentroidType.VALUE);
                result.put(c.getId(), c);
            }
            
            return result;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            centroids = createCentroids(clusters);
        }
        
        @Override
        public Feature map(Feature value) throws Exception {
            double minDistance = Double.MAX_VALUE;
            Centroid bestCentroid = null;
            for (Centroid centroid : centroids.values()) {
                double distance = centroid.distance(value);
                if (distance < minDistance) {
                    minDistance = distance;
                    bestCentroid = centroid;
                }
            }

            // Move the point part of the way to the best centroid cluster
            double newX = value.getX() - (value.getX() - bestCentroid.getFeature().getX()) * 0.3;
            double newY = value.getY() - (value.getY() - bestCentroid.getFeature().getY()) * 0.3;
            
            LOGGER.debug(String.format("Moving towards cluster %d (%f,%f): %f,%f => %f,%f", 
                    bestCentroid.getId(), bestCentroid.getFeature().getX(), bestCentroid.getFeature().getY(),
                    value.getX(), value.getY(), newX, newY));
            return new Feature(value.getId(), newX, newY, -1, bestCentroid.getId());
        }
        
        
    }
}
