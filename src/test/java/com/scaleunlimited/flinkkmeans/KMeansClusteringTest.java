package com.scaleunlimited.flinkkmeans;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
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
        List<Feature> features = null;
        try (InputStream is = KMeansClusteringTest.class.getResourceAsStream("/citibike-20180801-min.tsv")) {
            features = KMeansUtils.makeFeatures(is, 1000);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features, 10L);
        InMemorySinkFunction sink = new InMemorySinkFunction();
        
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2);
        
        final int numClusters = 20;
        double maxDistance = KMeansUtils.calcMaxDistance(features, numClusters);
        KMeansClustering.build(env, featuresSource, sink, numClusters, maxDistance, false);
        
        env.execute();
        
        Queue<FeatureResult> results = InMemorySinkFunction.getValues();
        
        while (!results.isEmpty()) {
            FeatureResult result = results.remove();
            int clusterId = result.getClusterId();
            Feature centroid = result.getCentroid();
            Feature f = result.getFeature();
            
            LOGGER.debug("Feature {} at {},{} assigned to cluster {} at {},{}", 
                    f.getId(), f.getX(), f.getY(),
                    clusterId, centroid.getX(), centroid.getY());
        }
    }
    
    @Test
    public void testFlinkClusteringData() throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2);

        String[] centers = KMeansData.INITIAL_CENTERS_2D.split("\n");
        List<Cluster> clusters = new ArrayList<>();
        Map<Integer, Cluster> clusterMap = new HashMap<>();
        for (String c : centers) {
            String[] fields = c.split("\\|");
            Feature f = new Feature(Double.parseDouble(fields[1]),
                    Double.parseDouble(fields[2]));
            Cluster cluster = new Cluster(Integer.parseInt(fields[0]), f);
            clusters.add(cluster);
            clusterMap.put(cluster.getId(), cluster);
            LOGGER.debug("Adding cluster {} at {},{}", cluster.getId(), f.getX(), f.getY());
        }
        
        // Keep track of which cluster we think each feature should be assigned to.
        Map<Integer, Integer> featureToTargetCluster = new HashMap<>();
        String[] points = KMeansData.DATAPOINTS_2D.split("\n");
        List<Feature> features = new ArrayList<>();
        
        for (String p : points) {
            String[] fields = p.split("\\|");
            
            double x = Double.parseDouble(fields[1]);
            double y = Double.parseDouble(fields[2]);
            Feature f = new Feature(Integer.parseInt(fields[0]), x, y);
            
            f = clusterize(clusters, f);
            featureToTargetCluster.put(f.getId(), findClosestCluster(clusters, f).getId());
            LOGGER.info("Adding feature {} at {},{}", f.getId(), f.getX(), f.getY());
            features.add(f);
        }
        
        double distance = KMeansUtils.calcMaxDistance(features, clusters.size());
        
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features);

        InMemorySinkFunction sink = new InMemorySinkFunction();
        KMeansClustering.build(env, featuresSource, sink, clusters, distance, false);
        
        FileUtils.write(new File("./target/kmeans-graph.dot"), FlinkUtils.planToDot(env.getExecutionPlan()));
        
        env.execute();
        
        Queue<FeatureResult> results = InMemorySinkFunction.getValues();
        
        int numResults = 0;
        
        // Keep track of unique feature ids - we should get one of each
        Set<Integer> featureIds = new HashSet<>();
        
        while (!results.isEmpty()) {
            numResults += 1;
            FeatureResult result = results.remove();
            int clusterId = result.getClusterId();
            Feature centroid = result.getCentroid();
            Feature f = result.getFeature();
            
            LOGGER.debug("Feature {} at {},{} assigned to cluster {} at {},{}", 
                    f.getId(), f.getX(), f.getY(),
                    clusterId, centroid.getX(), centroid.getY());
            
            if (!featureIds.add(f.getId())) {
                fail("Found duplicate feature id: " + f.getId());
            }
            
            // Which cluster did we think this should have wound up in.
            int targetClusterId = featureToTargetCluster.get(f.getId());
            if (clusterId != targetClusterId) {
                // Not what we were expecting, see if the actual distance is
                // less than the distance that we initially had to the target
                // cluster.
                double actualDistance = f.distance(centroid);
                double targetDistance = f.distance(clusterMap.get(targetClusterId).getCentroid());
                if (actualDistance > targetDistance) {
                    
                    fail(String.format("Got %d (%f), expected %d (%f) for %s\n", 
                            f.getClusterId(), actualDistance, 
                            targetClusterId, targetDistance,
                            f));
                }
            }
        }
        
        assertEquals(points.length, numResults);
    }

    /**
     * To create more realistic data, we'll perturb features (points) towards the
     * closest cluster
     * 
     * @param clusters List of starting clusters
     * @param value Feature to clusterize
     * @return modified Feature
     * @throws Exception
     */
    private Feature clusterize(List<Cluster> clusters, Feature value) {
        Cluster bestCluster = findClosestCluster(clusters, value);

        // Move the point towards the best cluster's location (centroid)
        double centroidX = bestCluster.getCentroid().getX();
        double centroidY = bestCluster.getCentroid().getY();
        double newX = centroidX + (value.getX() - centroidX) * 0.5;
        double newY = centroidY + (value.getY() - centroidY) * 0.5;
        return new Feature(value.getId(), newX, newY, -1);
    }

    private Cluster findClosestCluster(List<Cluster> clusters, Feature value) {
        double minDistance = Double.MAX_VALUE;
        Cluster bestCluster = null;
        for (Cluster cluster : clusters) {
            double distance = cluster.distance(value);
            if (distance < minDistance) {
                minDistance = distance;
                bestCluster = cluster;
            }
        }
        
        return bestCluster;
    }
    
    @Test
    public void testQueryableState() throws Exception {
        KMeansMiniCluster localCluster = new KMeansMiniCluster();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2);
        env.setMaxParallelism(localCluster.getMaxParallelism());

        final int numClusters = 2;
        List<Cluster> clusters = makeClusters(numClusters);

        // Note that we have to limit this to avoid deadlocking
        final int numPoints = numClusters * 1000;
        List<Feature> features = makeFeatures(clusters, numPoints);
        double maxDistance = KMeansUtils.calcMaxDistance(features, numClusters);
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features);

        InMemorySinkFunction sink = new InMemorySinkFunction();
        
        KMeansClustering.build(env, featuresSource, sink, numClusters, maxDistance, true);
        
        final Duration TEST_TIMEOUT = Duration.ofSeconds(10);
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        JobSubmissionResult submission = localCluster.start(deadline, env);

        QueryableStateClient client = new QueryableStateClient("localhost", 9069);
        client.setExecutionConfig(new ExecutionConfig());
        
        ValueStateDescriptor<Cluster> stateDescriptor =
                new ValueStateDescriptor<>(
                  "centroid",
                  TypeInformation.of(new TypeHint<Cluster>() {}));

        while (localCluster.isRunning()) {
            Thread.sleep(1000L);
            CompletableFuture<ValueState<Cluster>> resultFuture = client.getKvState(submission.getJobID(), "centroids", 0,
                    new TypeHint<Integer>() { }, stateDescriptor);
            resultFuture.thenAccept(response -> {
                try {
                    Cluster c = response.value();
                    System.out.println(c);
                } catch (Exception e) {
                    fail("Exception getting queryable state: " + e.getMessage());
                }
            });
        }
        
        Queue<FeatureResult> results = InMemorySinkFunction.getValues();
        assertEquals(features.size(), results.size());
    }

    private static List<Cluster> makeClusters(int numCentroids) {
        List<Cluster> result = new ArrayList<>(numCentroids);
        for (int i = 0; i < numCentroids; i++) {
            int x = ((i + 1) * 10);
            int y = ((i + 1) * 20);
            result.add(new Cluster(i, new Feature(x, y)));
        }

        return result;
    }

    private static List<Feature> makeFeatures(List<Cluster> clusters, int numFeatures) {
        int numClusters = clusters.size();
        Random rand = new Random(0L);
        List<Feature> result = new ArrayList<>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            double x = rand.nextDouble() * 10 * numClusters;
            double y = rand.nextDouble() * 20 * numClusters;
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
}
