package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Cluster implements Serializable {

    public static final int NO_CLUSTER_ID = -1;
    public static final double UNUSED_DISTANCE = Double.MAX_VALUE;
    
    private int id;
    
    // Average of all features that are part of our cluster.
    private Feature centroid;
    private int numFeatures;
    
    public Cluster(int clusterId) {
        this.id = clusterId;
        
        this.centroid = new Feature(clusterId, 0, 0);
        this.numFeatures = 0;
    }
    
    public Cluster(int clusterId, Feature centroid) {
        this(clusterId);
        addFeature(centroid);
    }

    public int getId() {
        return id;
    }
    
    public void setId(int id) {
        this.id = id;
    }

    public boolean isUnused() {
        return numFeatures == 0;
    }

    public double distance(Feature f) {
        if (numFeatures == 0) {
            return Cluster.UNUSED_DISTANCE;
        }

        return centroid.distance(f, numFeatures);
    }
    
    public void addFeature(Feature f) {
        centroid.plus(f);
        numFeatures++;
    }

    public void removeFeature(Feature f) {
        centroid.minus(f);
        numFeatures--;
    }

    public Feature getCentroid() {
        return new Feature(centroid, numFeatures);
    }
    
    @Override
    public String toString() {
        return String.format("Cluster %d (%s) with %d features", id, centroid.toString(), numFeatures);
    }

    
}