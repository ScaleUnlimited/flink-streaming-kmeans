package com.scaleunlimited.flinkkmeans;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansTool.class);

    private static final Duration TOOL_TIMEOUT = Duration.ofMinutes(15);
    
    private static KMeansMiniCluster _localCluster;
    
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    public static void main(String[] args) {
        
        KMeansToolOptions options = new KMeansToolOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        // Now see if we need to set up our own minicluster or not
        StreamExecutionEnvironment env;
        
        if (options.isLocal()) {
            _localCluster = new KMeansMiniCluster();
            env = StreamExecutionEnvironment.createLocalEnvironment(options.getParallelism(), KMeansMiniCluster.getConfig());
            env.setMaxParallelism(_localCluster.getMaxParallelism());
        } else {
            // TODO use options to get hostname, port
            env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 80, options.getParallelism());
        }
        
        List<Feature> features = new ArrayList<>();
        List<Centroid> centroids = new ArrayList<>();
        try (InputStream is = new FileInputStream(options.getInput())) {
            makeFeaturesAndCentroids(is, features, centroids, options.getNumClusters());
        } catch (Exception e) {
            System.err.println("Exception loading features: " + e.getMessage());
            System.exit(-1);
        }
        
        SourceFunction<Centroid> centroidsSource = new ParallelListSource<Centroid>(centroids);
        // TODO fix up streaming code so no delay is needed (optional, to slow down processing only)
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features, 10L);
        Server server = null;
        
        try {
            KMeansClustering.build(env, centroidsSource, featuresSource, new DiscardingSink<>());
            
            // TODO handle remote case, with no local cluster
            // TODO do we actually neeed a timeout in local mode?
            JobSubmissionResult submission = _localCluster.start(Deadline.now().plus(TOOL_TIMEOUT), env);
            LOGGER.info("Starting job with id " + submission.getJobID());
            
            ValueStateDescriptor<Centroid> stateDescriptor = new ValueStateDescriptor<>("centroid",
                    TypeInformation.of(new TypeHint<Centroid>() {}));

            // Set up Jetty server
            server = new Server(8085);
            // We assume the tool is running on the same server, but if we're not in local mode, then
            // we need to create a QueryableStateClient
            server.setHandler(new FlinkQueryStateHandler(_localCluster.getQueryableStateClient(),
                    stateDescriptor, submission.getJobID(), options.getNumClusters()));
            server.start();
            
            while (_localCluster.isRunning()) {
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
                    LOGGER.error("Exception shutting down Jetty server: " + e.getMessage(), e);
                }
            }
            
            if (_localCluster != null) {
                try {
                    _localCluster.stop();
                } catch (Exception e) {
                    LOGGER.error("Exception shutting down KMeansMiniCluster: " + e.getMessage(), e);
                }
            }
        }
        
    }

    public static void makeFeaturesAndCentroids(InputStream is, List<Feature> features,
            List<Centroid> centroids, int numCentroids) throws ParseException, IOException {
        
        // Get data, in <date time><tab><lat><tab><lon> format.
        // 2018-08-01 00:00:07.3210 40.78339981 -73.98093133
        List<String> rides = IOUtils.readLines(is, StandardCharsets.UTF_8);
        
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");
        
        int rideID = 0;
        double minLat = 180.0;
        double maxLat = -180.0;
        double minLon = 180.00;
        double maxLon = -180.00;
        
        for (String ride : rides) {
            String[] fields = ride.split("\t", 3);
            // TODO save time for generating watermark
            Date startTime = parser.parse(fields[0]);
            double lat = Double.parseDouble(fields[1]);
            double lon = Double.parseDouble(fields[2]);
            
            minLat = Math.min(lat,  minLat);
            maxLat = Math.max(lat, maxLat);
            minLon = Math.min(lon, minLon);
            maxLon = Math.max(lon, maxLon);
            
            Feature f = new Feature(rideID++, lat, lon);
            features.add(f);
        }

        Random rand = new Random(System.currentTimeMillis());
        double latCenter = (minLat + maxLat) / 2.0;
        double lonCenter = (minLon + maxLon) / 2.0;

        // Start off clusters more in the center
        double latRange = (maxLat - minLat) / 1.5;
        double lonRange = (maxLon - minLon) / 1.5;
        
        int centroidID = 0;
        for (int i = 0; i < numCentroids; i++) {
            Feature f = new Feature(latCenter + ((rand.nextDouble() - 0.5) * latRange),
                    lonCenter + ((rand.nextDouble() - 0.5) * lonRange));

            Centroid centroid = new Centroid(f, centroidID++, CentroidType.VALUE);
            centroids.add(centroid);
        }
    }
    
    private static class FlinkQueryStateHandler extends AbstractHandler {

        private QueryableStateClient _client;
        private ValueStateDescriptor<Centroid> _stateDescriptor;
        private JobID _jobID;
        private int _numClusters;
        
        public FlinkQueryStateHandler(QueryableStateClient client,
                ValueStateDescriptor<Centroid> stateDescriptor, JobID jobID,
                int numClusters) {
            super();
            
            _client = client;
            _stateDescriptor = stateDescriptor;
            _jobID = jobID;
            _numClusters = numClusters;
        }
        
        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            response.setContentType("application/json; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setHeader("Access-Control-Allow-Origin", "*");
            
            PrintWriter writer = response.getWriter();
            StringBuilder out = new StringBuilder();
            out.append("{\n\t\"type\": \"FeatureCollection\",\n");
            out.append("\t\"features\": [");

            Queue<Centroid> centroids = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < _numClusters; i++) {
                CompletableFuture<ValueState<Centroid>> resultFuture = _client.getKvState(_jobID, "centroids", i,
                        new TypeHint<Integer>() { }, _stateDescriptor);

                try {
                    Centroid c = resultFuture.get().value();
                    centroids.add(c);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownKeyOrNamespaceException) {
                        // Ignore this error, as it happens when the flow hasn't generated results yet, so
                        // we want to just return an empty result.
                        LOGGER.debug("Can't get results yet for centroid {}", i);
                    } else {
                        LOGGER.error("Error getting centroid data: " + e.getMessage(), e);
                    }
                    
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
    
    public static class KMeansToolOptions {
        private boolean _local = false;
        private String _input;
        private int _parallelism = 2;
        private int _numClusters = 10;
        
        @Option(name = "-local", usage = "run a local minicluster", required = false)
        public void setLocal(boolean local) {
            _local = local;
        }
        
        public boolean isLocal() {
            return _local;
        }

        @Option(name = "-input", usage = "path to tsv file that contains start time, lat, lon", required = true)
        public void setInput(String inputPath) {
            _input = inputPath;
        }
        
        public String getInput() {
            return _input;
        }
        
        @Option(name = "-parallelism", usage = "set parallelism", required = false)
        public void setParallelism(int parallelism) {
            _parallelism = parallelism;
        }
        
        public int getParallelism() {
            return _parallelism;
        }

        @Option(name = "-clusters", usage = "set number of clusters", required = false)
        public void setNumClusters(int numClusters) {
            _numClusters = numClusters;
        }
        
        public int getNumClusters() {
            return _numClusters;
        }


    }
}
