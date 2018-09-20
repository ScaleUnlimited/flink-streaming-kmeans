package com.scaleunlimited.flinkkmeans;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironmentWithAsyncExecution;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansTool.class);

    private static final int NUM_CENTROIDS = 2;
    
    public static void main(String[] args) {
        final LocalStreamEnvironmentWithAsyncExecution env = 
                new LocalStreamEnvironmentWithAsyncExecution();
        
        List<Centroid> centroids = makeCentroids(NUM_CENTROIDS);
        SourceFunction<Centroid> centroidsSource = new ParallelListSource<Centroid>(centroids);

        // Note that we have to limit this to avoid deadlocking
        final int numPoints = NUM_CENTROIDS * 1000;
        List<Feature> features = makeFeatures(centroids, numPoints);
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features, 10L);
        Server server = null;

        try {
            KMeansClustering.build(env, centroidsSource, featuresSource, new DiscardingSink<>());
            JobSubmissionResult submission = env.executeAsync("testQueryableState");
            
            QueryableStateClient client = new QueryableStateClient("localhost", 9069);
            client.setExecutionConfig(new ExecutionConfig());
            ValueStateDescriptor<Centroid> stateDescriptor = new ValueStateDescriptor<>("centroid",
                    TypeInformation.of(new TypeHint<Centroid>() {}));

            // Set up Jetty server
            server = new Server(8080);
            ContextHandler contextFeatures = new ContextHandler("/features");
            contextFeatures.setHandler(new FlinkQueryStateHandler(client, stateDescriptor, submission.getJobID()));

            ContextHandler contextMap = new ContextHandler("/map");
            contextMap.setHandler(new MapRequestHandler());

            ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(new Handler[] { contextFeatures, contextMap });

            server.setHandler(contexts);
            server.start();
            // server.join();
            
            while (env.isRunning(submission.getJobID())) {
                Thread.sleep(1000L);
            }
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace();
            System.exit(-1);
        } finally {
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
        
    }

    // Remove me
    static final double MIN_LATITUDE = 40.57;
    static final double MAX_LATITUDE = 40.77;
    
    static final double MIN_LONGITUDE = -74.10;
    static final double MAX_LONGITUDE = -73.90;

    // TODO Remove me
    private static List<Centroid> makeCentroids(int numCentroids) {
        List<Centroid> result = new ArrayList<>(numCentroids);
        Random rand = new Random(System.currentTimeMillis());
        
        double deltaLat = MAX_LATITUDE - MIN_LATITUDE;
        double deltaLon = MAX_LONGITUDE - MIN_LONGITUDE;
        
        for (int i = 0; i < numCentroids; i++) {
            double x = MIN_LONGITUDE + (rand.nextDouble() * deltaLon);
            double y = MIN_LATITUDE + (rand.nextDouble() * deltaLat);
            
            result.add(new Centroid(new Feature(x, y), i, CentroidType.VALUE));
        }

        return result;
    }

    // TODO Remove me
    private static List<Feature> makeFeatures(List<Centroid> centroids, int numFeatures) {
        int numCentroids = centroids.size();
        Random rand = new Random(System.currentTimeMillis());
        
        double deltaLat = (MAX_LATITUDE - MIN_LATITUDE) * 0.1;
        double deltaLon = (MAX_LONGITUDE - MIN_LONGITUDE) * 0.1;

        List<Feature> result = new ArrayList<>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            Centroid centroid = centroids.get(rand.nextInt(numCentroids));
            
            double x = centroid.getFeature().getX() + ((rand.nextDouble() - 0.5) * deltaLon);
            double y = centroid.getFeature().getY() + ((rand.nextDouble() - 0.5) * deltaLat);
            result.add(new Feature(i, x, y));
        }

        return result;
    }


    private static class FlinkQueryStateHandler extends AbstractHandler {

        private QueryableStateClient _client;
        private ValueStateDescriptor<Centroid> _stateDescriptor;
        private JobID _jobID;

        public FlinkQueryStateHandler(QueryableStateClient client,
                ValueStateDescriptor<Centroid> stateDescriptor, JobID jobID) {
            super();
            
            _client = client;
            _stateDescriptor = stateDescriptor;
            _jobID = jobID;
        }
        
        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            response.setContentType("application/json; charset=utf-8");
//            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setStatus(HttpServletResponse.SC_OK);

            PrintWriter writer = response.getWriter();
            StringBuilder out = new StringBuilder();
            out.append("{\n\t\"type\": \"FeatureCollection\",\n");
            out.append("\t\"features\": [");

            Queue<Centroid> centroids = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < NUM_CENTROIDS; i++) {
                CompletableFuture<ValueState<Centroid>> resultFuture = _client.getKvState(_jobID, "centroids", i,
                        new TypeHint<Integer>() { }, _stateDescriptor);

                try {
                    Centroid c = resultFuture.get().value();
                    centroids.add(c);
                } catch (ExecutionException e) {
                    // Ignore this error, as it happens when the flow hasn't generated results yet, so
                    // we want to just return an empty result.
                    LOGGER.debug("Can't get results yet", e);
                    break;
                } catch (Exception e) {
                    LOGGER.error("Error getting centroid data: " + e.getMessage(), e);

                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    writer.println(String.format("{ \"error\": \"%s\"", e.getMessage()));

                    baseRequest.setHandled(true);
                    return;
                }
            }

            boolean firstCentroid = true;
            for (Centroid c : centroids) {
                if (!firstCentroid) {
                    out.append(",\n\t\t");
                } else {
                    firstCentroid = false;
                }

                printCentroid(out, c);
            }
            
            out.append("\n\t]\n");
            out.append("}\n");
            writer.print(out.toString());
            baseRequest.setHandled(true);
        }

        private void printCentroid(StringBuilder out, Centroid c) {
            double longitude = c.getFeature().getX();
            double latitude = c.getFeature().getY();
            
            out.append("{\n");
            out.append("\t\t\t\"type\": \"Feature\",\n");
            out.append("\t\t\t\"geometry\": {\n");
            out.append("\t\t\t\t\"type\": \"Point\",\n");
            out.append(String.format("\t\t\t\t\"coordinates\": [%f, %f]\n", longitude, latitude));
            out.append("\t\t\t}\n");
            out.append("\t\t}");
        }
    }

    private static class MapRequestHandler extends AbstractHandler {

        String _mapFile;
        
        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            response.setContentType("text/html; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            if (_mapFile == null) {
                _mapFile = IOUtils.toString(KMeansTool.class.getResourceAsStream("/nyc-bike-share.html"), StandardCharsets.UTF_8.name());
            }
            PrintWriter writer = response.getWriter();
            
            writer.print(_mapFile);
            baseRequest.setHandled(true);
        }
    }
}
