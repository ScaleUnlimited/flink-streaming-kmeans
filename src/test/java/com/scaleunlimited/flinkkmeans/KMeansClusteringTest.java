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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
        
        Queue<CentroidFeature> results = InMemorySinkFunction.getValues();
        assertEquals(points.length, results.size());
        
        Map<Integer, Centroid> clusters = ClusterizePoints.createCentroids(points);
        
        while (!results.isEmpty()) {
            CentroidFeature result = results.remove();
            Centroid c = result.getCentroid();
            Feature f = result.getFeature();
            LOGGER.info("Feature {} at {},{} assigned to centroid {} at {},{}", 
                    f.getId(), f.getX(), f.getY(),
                    c.getId(), c.getFeature().getX(), c.getFeature().getY());
            
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

    /**
     * To create more realistic data, we'll perturb features (points) towards the
     * closest centroid (cluster)
     * 
     * @param centroids List of starting centroids
     * @param value Feature to clusterize
     * @return modified Feature
     * @throws Exception
     */
    private Feature clusterize(List<Centroid> centroids, Feature value) {
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
        return new Feature(value.getId(), newX, newY, -1, bestCentroid.getId());
    }

    @Test
    public void testSyntheticData() throws Exception {
        final StreamExecutionEnvironment env = 
                StreamExecutionEnvironment.createLocalEnvironment(2);

        final int numCentroids = 2;
        List<Centroid> centroids = makeCentroids(numCentroids);
        SourceFunction<Centroid> centroidsSource = new ParallelListSource<Centroid>(centroids);

        final int numPoints = numCentroids * 100;
        List<Feature> features = makeFeatures(centroids, numPoints);
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features);

        InMemorySinkFunction sink = new InMemorySinkFunction();
        
        KMeansClustering.build(env, centroidsSource, featuresSource, sink);
        env.execute();

        Queue<CentroidFeature> results = InMemorySinkFunction.getValues();
        assertEquals(features.size(), results.size());
    }

    private static List<Centroid> makeCentroids(int numCentroids) {
        List<Centroid> result = new ArrayList<>(numCentroids);
        for (int i = 0; i < numCentroids; i++) {
            int x = ((i + 1) * 10);
            int y = ((i + 1) * 20);
            result.add(new Centroid(new Feature(x, y), i, CentroidType.VALUE));
        }

        return result;
    }

    private static List<Feature> makeFeatures(List<Centroid> centroids, int numFeatures) {
        int numCentroids = centroids.size();
        Random rand = new Random(0L);
        List<Feature> result = new ArrayList<>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            double x = rand.nextDouble() * 10 * numCentroids;
            double y = rand.nextDouble() * 20 * numCentroids;
            result.add(new Feature(i, x, y));
        }

        return result;
    }

    @SuppressWarnings("serial")
    private static class InMemorySinkFunction extends RichSinkFunction<CentroidFeature> {
        
        // Static, so all parallel functions will write to the same queue
        static private Queue<CentroidFeature> _values = new ConcurrentLinkedQueue<>();
        
        public static Queue<CentroidFeature> getValues() {
            return _values;
        }
        
        @Override
        public void invoke(CentroidFeature value) throws Exception {
            LOGGER.debug("Adding feature {} for centroid {}", value.getFeature(), value.getCentroid());
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
