package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Cluster implements Serializable {

    public static final int NO_CLUSTER_ID = -1;
    public static final double UNUSED_DISTANCE = Double.MAX_VALUE;
    
    private int id;
    private Feature centroid;   // Sum of all features that are part of our cluster.
    private int numFeatures;    // So we can calculate the real (average) centroid.
    
    public Cluster() {
        // So it's a valid POJO.
    }
    
    public Cluster(int clusterId) {
        this.id = clusterId;
        
        this.centroid = new Feature(Feature.NO_FEATURE_ID, 0, 0);
        this.numFeatures = 0;
    }
    
    public Cluster(int clusterId, Feature centroid) {
        this(clusterId);
        addFeature(centroid);
    }

    public Cluster(Cluster cluster) {
        this.id = cluster.id;
        this.centroid = new Feature(cluster.centroid);
        this.numFeatures = cluster.numFeatures;
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

    /**
     * Calculate the actual centroid (average the feature values) and
     * return a copy.
     * 
     * @return
     */
    public Feature getCentroid() {
        return new Feature(centroid, numFeatures);
    }
    
    public void setCentroid(Feature centroid) {
        throw new RuntimeException("You can't set the centroid");
    }
    
    public int getNumFeatures() {
        return numFeatures;
    }
    
    public void setNumFeatures(int size) {
        throw new RuntimeException("You can't set the number of features");
    }
    
    @Override
    public String toString() {
        return String.format("Cluster %d (%s) with %d features", id, centroid.toString(), numFeatures);
    }

    
}