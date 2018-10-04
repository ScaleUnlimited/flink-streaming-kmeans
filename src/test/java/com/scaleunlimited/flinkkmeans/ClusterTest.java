package com.scaleunlimited.flinkkmeans;

import static org.junit.Assert.*;

import org.junit.Test;

public class ClusterTest {

    @Test
    public void testRemoveBeforeAddd() {
        final int clusterId = 23;
        Cluster c = new Cluster(clusterId);

        final Feature f1 = new Feature(1, 3, 5);
        final Feature f2 = new Feature(2, 7, 11);
        final Feature f3 = new Feature(3, 6, 6);
        
        assertEquals(Cluster.UNUSED_DISTANCE, c.distance(f3), 0.01);
        c.removeFeature(f1);
        assertEquals(Cluster.UNUSED_DISTANCE, c.distance(f3), 0.01);
        
        c.addFeature(f2);
        assertEquals(Cluster.UNUSED_DISTANCE, c.distance(f3), 0.01);

        c.addFeature(f1);
        assertEquals(0.0, c.distance(f2), 0.01);
        
        c.removeFeature(f2);
        assertEquals(Cluster.UNUSED_DISTANCE, c.distance(f3), 0.01);
    }

}
