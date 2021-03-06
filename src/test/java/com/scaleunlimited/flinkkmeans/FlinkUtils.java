package com.scaleunlimited.flinkkmeans;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;

public class FlinkUtils {

    private static class FlinkPlanEdge {
        private int id;
        private String ship_strategy;
        @SuppressWarnings("unused")
        private String side;

        public int getId() {
            return id;
        }

        public String getShip_strategy() {
            return ship_strategy;
        }
    }

    private static class FlinkPlanNode {
        private static final FlinkPlanEdge[] NO_EDGES = new FlinkPlanEdge[0];

        private int id;
        private String type;
        @SuppressWarnings("unused")
        private String pact;
        private String contents;
        private int parallelism;
        private FlinkPlanEdge[] predecessors;

        public int getId() {
            return id;
        }

        public String getType() {
            return type;
        }

        public String getContents() {
            return contents;
        }

        public int getParallelism() {
            return parallelism;
        }

        public FlinkPlanEdge[] getPredecessors() {
            if (predecessors == null) {
                return NO_EDGES;
            } else {
                return predecessors;
            }
        }
    }

    private static class FlinkPlan {
        private FlinkPlanNode[] nodes;

        public FlinkPlanNode[] getNodes() {
            return nodes;
        }
    }

    /**
     * Given a JSON representation of a Flink Topology (from StreamExecutionEnvironment#getExecutionPlan), convert it to
     * a standard .dot format suitable for visualizing with OmniGraffle and other programs.
     * 
     * See http://www.graphviz.org/doc/info/lang.html See http://www.graphviz.org/doc/info/attrs.html
     * 
     * @param plan
     *            JSON version of plan
     * @return dot format graph.
     */
    public static String planToDot(String plan) {
        FlinkPlan flinkPlan = new Gson().fromJson(plan, FlinkPlan.class);

        StringBuilder result = new StringBuilder("digraph G {\n");

        // Keep track of iteration sources, which are implicit. So we map from
        // the iteration number to the source id
        Map<Integer, Integer> iterationMap = new HashMap<Integer, Integer>();

        final Pattern ITERATION_SOURCE_NAME = Pattern.compile("IterationSource\\-(\\d+)");
        final Pattern ITERATION_SINK_NAME = Pattern.compile("IterationSink\\-(\\d+)");

        final Pattern SOURCE_OR_SINK_NAME = Pattern.compile("(Source|Sink): (.*)");

        FlinkPlanNode[] nodes = flinkPlan.getNodes();
        for (FlinkPlanNode node : nodes) {
            Matcher m = SOURCE_OR_SINK_NAME.matcher(node.getType());
            boolean isSourceOrSink = m.matches();
            String nodeName = isSourceOrSink ? m.group(2) : node.getContents();
            String nodeShape = isSourceOrSink ? "box" : "ellipse";

            boolean isIterationSourceOrSink = ITERATION_SOURCE_NAME.matcher(node.getType())
                    .matches() || ITERATION_SINK_NAME.matcher(node.getType()).matches();
            String fillColor = isSourceOrSink || isIterationSourceOrSink ? "8EFF4C" : "FFFFFF";

            result.append(
                    String.format("  %d [shape=\"%s\", fillcolor=\"#%s\", label=\"%s (%d)\"];\n",
                            node.getId(), nodeShape, fillColor, nodeName, node.getParallelism()));

            m = ITERATION_SOURCE_NAME.matcher(node.getType());
            if (m.matches()) {
                // Map from iteration number to the source node id
                iterationMap.put(Integer.parseInt(m.group(1)), node.getId());
            }
        }

        // Now dump out the edges
        // 1 -> 2 [label = "blah"];
        for (FlinkPlanNode node : nodes) {
            int nodeId = node.getId();
            FlinkPlanEdge[] edges = node.getPredecessors();
            for (FlinkPlanEdge edge : edges) {
                result.append(String.format("  %d -> %d [label = \"%s\"];\n", edge.getId(), nodeId,
                        edge.getShip_strategy()));
            }

            // Now check if this node is an iteration sink. If so, add an explicit edge
            // from it to the corresponding source node.
            Matcher m = ITERATION_SINK_NAME.matcher(node.getType());
            if (m.matches()) {
                int iterationID = Integer.parseInt(m.group(1));
                result.append(String.format("  %d -> %d [label = \"ITERATION\"];\n", node.getId(),
                        iterationMap.get(iterationID)));
            }
        }

        result.append("}\n");
        return result.toString();
    }
}
