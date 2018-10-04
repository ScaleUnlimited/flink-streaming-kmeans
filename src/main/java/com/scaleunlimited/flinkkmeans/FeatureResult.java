package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FeatureResult implements Serializable {

    private Feature feature;
    private int clusterId;
    private Feature centroid;
    
    public FeatureResult() {
        // So Flink treats it as a POJO
    }
    
    public FeatureResult(Feature feature, Cluster cluster) {
        this.feature = new Feature(feature);
        this.clusterId = cluster.getId();
        this.centroid = new Feature(cluster.getCentroid());
    }

    public FeatureResult(FeatureResult cf) {
        this.feature = new Feature(cf.feature);
        this.clusterId = cf.clusterId;
        this.centroid = new Feature(cf.centroid);
    }
    
    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        throw new RuntimeException("You can't set the cluster id");
    }
    
    public Feature getCentroid() {
        return centroid;
    }
    
    public void setCentroid(Feature centroid) {
        throw new RuntimeException("You can't set the centroid");
    }
    
    public Feature getFeature() {
        return feature;
    }
    
    public void setFeature(Feature feature) {
        throw new RuntimeException("You can't set the feature");
    }
    
    @Override
    public String toString() {
        return String.format("%d (%s) | %s", clusterId, centroid, feature);
    }
}
