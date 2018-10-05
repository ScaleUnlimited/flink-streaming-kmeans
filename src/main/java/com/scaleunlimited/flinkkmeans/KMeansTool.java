package com.scaleunlimited.flinkkmeans;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KMeansTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(KMeansTool.class);

    private static final Duration TOOL_TIMEOUT = Duration.ofMinutes(15);
    
    private static KMeansMiniCluster _localCluster;
    
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
        
        List<Feature> features = null;
        try (InputStream is = new FileInputStream(options.getInput())) {
            features = KMeansUtils.makeFeatures(is);
        } catch (Exception e) {
            System.err.println("Exception loading features: " + e.getMessage());
            System.exit(-1);
        }
        
        // TODO fix up streaming code so no delay is needed (optional, to slow down processing only)
        SourceFunction<Feature> featuresSource = new ParallelListSource<Feature>(features, 10L);
        Server server = null;
        
        try {
            double maxDistance = KMeansUtils.calcMaxDistance(features, options.getNumClusters());
            KMeansClustering.build(env, featuresSource, new DiscardingSink<>(), options.getNumClusters(), maxDistance);
            
            // TODO handle remote case, with no local cluster
            // TODO do we actually neeed a timeout in local mode?
            JobSubmissionResult submission = _localCluster.start(Deadline.now().plus(TOOL_TIMEOUT), env);
            LOGGER.info("Starting job with id " + submission.getJobID());
            
            ValueStateDescriptor<Feature> stateDescriptor = new ValueStateDescriptor<>("centroid",
                    TypeInformation.of(new TypeHint<Feature>() {}));

            // Set up Jetty server
            server = new Server(8085);
            // We assume the tool is running on the same server, but if we're not in local mode, then
            // we need to create a QueryableStateClient
            ContextHandler contextClusters = new ContextHandler("/clusters");
            contextClusters.setHandler(new ClustersRequestHandler(_localCluster.getQueryableStateClient(),
                    stateDescriptor, submission.getJobID(), options.getNumClusters()));

            ContextHandler contextFeatures = new ContextHandler("/features");
            contextFeatures.setHandler(new FeaturesRequestHandler());

            ContextHandler contextMap = new ContextHandler("/map");
            contextMap.setHandler(new MapRequestHandler(options.getAccessToken()));

            ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(new Handler[] { contextClusters, contextFeatures, contextMap });

            server.setHandler(contexts);
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

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    private static class ClustersRequestHandler extends AbstractHandler {

        private QueryableStateClient _client;
        private ValueStateDescriptor<Feature> _stateDescriptor;
        private JobID _jobID;
        private int _numClusters;
        
        public ClustersRequestHandler(QueryableStateClient client,
                ValueStateDescriptor<Feature> stateDescriptor, JobID jobID,
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

            Queue<Feature> centroids = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < _numClusters; i++) {
                CompletableFuture<ValueState<Feature>> resultFuture = _client.getKvState(_jobID, "centroids", i,
                        new TypeHint<Integer>() { }, _stateDescriptor);

                try {
                    Feature centroid = resultFuture.get().value();
                    centroids.add(centroid);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownKeyOrNamespaceException) {
                        // Ignore this error, as it happens when the flow hasn't generated results yet, so
                        // we want to just return an empty result.
                        LOGGER.debug("Can't get results yet for cluster {}", i);
                    } else {
                        LOGGER.error("Error getting cluster data: " + e.getMessage(), e);
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

            boolean firstCluster = true;
            for (Feature centroid : centroids) {
                if (!firstCluster) {
                    out.append(",\n\t\t");
                } else {
                    firstCluster = false;
                }

                printClusterCentroid(out, centroid);
            }
            
            out.append("\n\t]\n");
            out.append("}\n");
            writer.print(out.toString());
            baseRequest.setHandled(true);
        }

        private void printClusterCentroid(StringBuilder out, Feature centroid) {
            double longitude = centroid.getX();
            double latitude = centroid.getY();
            
            out.append("{\n");
            out.append("\t\t\t\"type\": \"Feature\",\n");
            out.append("\t\t\t\"geometry\": {\n");
            out.append("\t\t\t\t\"type\": \"Point\",\n");
            out.append(String.format("\t\t\t\t\"coordinates\": [%f, %f]\n", longitude, latitude));
            out.append("\t\t\t}\n");
            out.append("\t\t}");
        }
    }

    private static class FeaturesRequestHandler extends AbstractHandler {

        
        public FeaturesRequestHandler() {
        }
        
        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            response.setContentType("text/html; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            

            PrintWriter writer = response.getWriter();
            writer.print("/features has not yet been implemented");
            baseRequest.setHandled(true);
        }
    }

    private static class MapRequestHandler extends AbstractHandler {

        private static String ACCESS_TOKEN_TO_REPLACE = "__MAPBOX_ACCESS_TOKEN__";
        private String _mapFile;
        private String _accessToken;
        
        public MapRequestHandler(String accessToken) {
            _accessToken = accessToken;
        }
        
        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            response.setContentType("text/html; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            if (_mapFile == null) {
                String fileStr = IOUtils.toString(KMeansTool.class.getResourceAsStream("/nyc-bike-share.html"), StandardCharsets.UTF_8.name());
                _mapFile = fileStr.replace(ACCESS_TOKEN_TO_REPLACE, _accessToken);
            }
            PrintWriter writer = response.getWriter();
            writer.print(_mapFile);
            baseRequest.setHandled(true);
        }
    }

    public static class KMeansToolOptions {
        private String _accessToken;
        private boolean _local = false;
        private String _input;
        private int _parallelism = 2;
        private int _numClusters = 10;
        
        @Option(name = "-accesstoken", usage = "MapBox access token", required = true)
        public void setAccessToken(String accessToken) {
            _accessToken = accessToken;
        }
        
        public String getAccessToken() {
            return _accessToken;
        }

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
