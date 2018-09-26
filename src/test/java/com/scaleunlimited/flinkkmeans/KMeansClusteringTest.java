package com.scaleunlimited.flinkkmeans;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansClusteringTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansClusteringTest.class);

    @Test
    public void testCitiBike() throws Exception {
        List<Feature> features = new ArrayList<>();
        List<Centroid> centroids = new ArrayList<>();
        try (InputStream is = KMeansClusteringTest.class.getResourceAsStream("/citibike-20180801-min.tsv")) {
            KMeansTool.makeFeaturesAndCentroids(is, features, centroids, 10);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features, 10L);
        SourceFunction<Centroid> centroidsSource = new ParallelListSource<Centroid>(centroids);

        InMemorySinkFunction sink = new InMemorySinkFunction();
        
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2);
        KMeansClustering.build(env, centroidsSource, featuresSource, sink);
        
        env.execute();
        
        Queue<FeatureResult> results = InMemorySinkFunction.getValues();
        
        int numResults = 0;
        while (!results.isEmpty()) {
            numResults += 1;
            FeatureResult result = results.remove();
            Centroid c = result.getCentroid();
            Feature f = result.getFeature();
            
            LOGGER.debug("Feature {} at {},{} assigned to centroid {} at {},{}", 
                    f.getId(), f.getX(), f.getY(),
                    c.getId(), c.getFeature().getX(), c.getFeature().getY());
        }
    }
    
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
            Centroid centroid = new Centroid(f, Integer.parseInt(fields[0]), CentroidType.VALUE);
            centroids.add(centroid);
            LOGGER.debug("Adding centroid {} at {},{}", centroid.getId(), f.getX(), f.getY());
        }
        
        SourceFunction<Centroid> centroidsSource = new ParallelListSource<Centroid>(centroids);
        
        Map<Integer, Integer> featureToTargetCentroid = new HashMap<>();
        String[] points = KMeansData.DATAPOINTS_2D.split("\n");
        List<Feature> features = new ArrayList<>();
        for (String p : points) {
            String[] fields = p.split("\\|");
            
            Feature f = new Feature(
                        Integer.parseInt(fields[0]),
                        Double.parseDouble(fields[1]),
                        Double.parseDouble(fields[2]));
            
            f = clusterize(centroids, f);
            featureToTargetCentroid.put(f.getId(), findClosestCentroid(centroids, f).getId());
            LOGGER.info("Adding feature {} at {},{}", f.getId(), f.getX(), f.getY());
            features.add(f);
        }
        
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features);

        InMemorySinkFunction sink = new InMemorySinkFunction();
        KMeansClustering.build(env, centroidsSource, featuresSource, sink);
        
        FileUtils.write(new File("./target/kmeans-graph.dot"), FlinkUtils.planToDot(env.getExecutionPlan()));
        
        env.execute();
        
        Queue<FeatureResult> results = InMemorySinkFunction.getValues();
        
        Map<Integer, Centroid> clusters = createCentroids(points);
        
        int numResults = 0;
        Set<Integer> featureIds = new HashSet<>();
        
        while (!results.isEmpty()) {
            numResults += 1;
            FeatureResult result = results.remove();
            Centroid c = result.getCentroid();
            Feature f = result.getFeature();
            
            LOGGER.debug("Feature {} at {},{} assigned to centroid {} at {},{}", 
                    f.getId(), f.getX(), f.getY(),
                    c.getId(), c.getFeature().getX(), c.getFeature().getY());
            
            if (!featureIds.add(f.getId())) {
                fail("Found duplicate feature id: " + f.getId());
            }
            
            int targetCentroidId = featureToTargetCentroid.get(f.getId());
            if (f.getCentroidId() != targetCentroidId) {
                double actualDistance = f.distance(c.getFeature());
                double targetDistance = f.distance(clusters.get(targetCentroidId).getFeature());
                if (actualDistance > targetDistance) {
                    fail(String.format("Got %d (%f), expected %d (%f) for %s\n", 
                            f.getCentroidId(), actualDistance, 
                            targetCentroidId, targetDistance,
                            f));
                }
            }
        }
        
        assertEquals(points.length, numResults);
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
        Centroid bestCentroid = findClosestCentroid(centroids, value);

        // Move the point part of the way to the best centroid cluster
        double newX = value.getX() - (value.getX() - bestCentroid.getFeature().getX()) * 0.3;
        double newY = value.getY() - (value.getY() - bestCentroid.getFeature().getY()) * 0.3;
        return new Feature(value.getId(), newX, newY, -1);
    }

    private Centroid findClosestCentroid(List<Centroid> centroids, Feature value) {
        double minDistance = Double.MAX_VALUE;
        Centroid bestCentroid = null;
        for (Centroid centroid : centroids) {
            double distance = centroid.distance(value);
            if (distance < minDistance) {
                minDistance = distance;
                bestCentroid = centroid;
            }
        }
        
        return bestCentroid;
    }
    
    @Test
    public void testQueryableState() throws Exception {
        KMeansMiniCluster localCluster = new KMeansMiniCluster();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2);
        env.setMaxParallelism(localCluster.getMaxParallelism());

        final int numCentroids = 2;
        List<Centroid> centroids = makeCentroids(numCentroids);
        SourceFunction<Centroid> centroidsSource = new ParallelListSource<Centroid>(centroids);

        // Note that we have to limit this to avoid deadlocking
        final int numPoints = numCentroids * 1000;
        List<Feature> features = makeFeatures(centroids, numPoints);
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features);

        InMemorySinkFunction sink = new InMemorySinkFunction();
        
        KMeansClustering.build(env, centroidsSource, featuresSource, sink);
        
        final Duration TEST_TIMEOUT = Duration.ofSeconds(10);
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        JobSubmissionResult submission = localCluster.start(deadline, env);

        QueryableStateClient client = new QueryableStateClient("localhost", 9069);
        client.setExecutionConfig(new ExecutionConfig());
        
        ValueStateDescriptor<Centroid> stateDescriptor =
                new ValueStateDescriptor<>(
                  "centroid",
                  TypeInformation.of(new TypeHint<Centroid>() {}));

        while (localCluster.isRunning()) {
            Thread.sleep(1000L);
            CompletableFuture<ValueState<Centroid>> resultFuture = client.getKvState(submission.getJobID(), "centroids", 0,
                    new TypeHint<Integer>() { }, stateDescriptor);
            resultFuture.thenAccept(response -> {
                try {
                    Centroid c = response.value();
                    System.out.println(c);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        
        Queue<FeatureResult> results = InMemorySinkFunction.getValues();
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
    private static class InMemorySinkFunction extends RichSinkFunction<FeatureResult> {
        
        // Static, so all parallel functions will write to the same queue
        static private Queue<FeatureResult> _values = new ConcurrentLinkedQueue<>();
        
        public InMemorySinkFunction() {
            _values.clear();
        }
        
        public static Queue<FeatureResult> getValues() {
            return _values;
        }
        
        @Override
        public void invoke(FeatureResult value) throws Exception {
            LOGGER.debug("Adding {} for {}", value.getFeature(), value.getCentroid());
            _values.add(new FeatureResult(value));
        }
    }

    private static Map<Integer, Centroid> createCentroids(String[] points) {
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

}
