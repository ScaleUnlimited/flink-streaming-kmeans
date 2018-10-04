package com.scaleunlimited.flinkkmeans;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.junit.Test;
import org.mockito.Mockito;

public class RandomFeatureSourceTest {

    @Test
    public void test() throws Exception {
        final int numClusters = 5;
        
        List<Cluster> clusters = new ArrayList<>();
        for (int i = 0; i < numClusters; i++) {
            Feature f = new Feature(i * 10, i * 20);
            clusters.add(new Cluster(i, f));
        }
        
        final int numFeatures = 100;
        final double spread = 5.0;
        
        final double minXValue = -15.0;
        final double maxXValue = (numClusters - 1) * 10.0 + 15.0;
        final double minYValue = -15.0;
        final double maxYValue = (numClusters - 1) * 20.0 + 15.0;

        RandomFeatureSource source = new RandomFeatureSource(clusters, numFeatures, spread);
        StreamingRuntimeContext ctx = Mockito.mock(StreamingRuntimeContext.class);
        Mockito.when(ctx.getNumberOfParallelSubtasks()).thenReturn(2);
        Mockito.when(ctx.getIndexOfThisSubtask()).thenReturn(1);
        
        source.setRuntimeContext(ctx);
        Configuration conf = Mockito.mock(Configuration.class);
        source.open(conf);
        
        for (int i = 0; i < numFeatures; i++) {
            Feature f = source.makeRandomFeature();
            
            double x = f.getX();
            assertTrue(x >= minXValue);
            assertTrue(x <= maxXValue);
            double y = f.getY();
            assertTrue(y >= minYValue);
            assertTrue(y <= maxYValue);
        }
    }

}
