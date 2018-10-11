package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FeatureResult implements Serializable {

    private Feature feature;
    private Cluster cluster;
    
    public FeatureResult() {
        // So Flink treats it as a POJO
    }
    
    public FeatureResult(Feature feature, Cluster cluster) {
        this.feature = new Feature(feature);
        this.cluster = new Cluster(cluster);
    }

    public FeatureResult(FeatureResult cf) {
        this.feature = new Feature(cf.feature);
        this.cluster = new Cluster(cf.cluster);
    }
    
    public Cluster getCluster() {
        return cluster;
    }
    
    public void setCluster(Cluster cluster) {
        throw new RuntimeException("You can't set the cluster");
    }
    
    public int getClusterId() {
        return cluster.getId();
    }

    public void setClusterId(int clusterId) {
        throw new RuntimeException("You can't set the cluster id");
    }
    
    public Feature getCentroid() {
        return cluster.getCentroid();
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
    
    public int getClusterSize() {
        return cluster.getNumFeatures();
    }
    
    public void setClusterSize(int clusterSize) {
        throw new RuntimeException("You can't set the cluster size");
    }
    
    @Override
    public String toString() {
        return String.format("%d (%s) | %s", getClusterId(), getCentroid(), feature);
    }
}
