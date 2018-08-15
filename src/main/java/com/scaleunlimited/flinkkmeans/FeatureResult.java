package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FeatureResult implements Serializable {

    private Feature feature;
    private Centroid centroid;
    
    public FeatureResult(Feature feature, Centroid centroid) {
        this.centroid = new Centroid(centroid);
        this.feature = new Feature(feature);
    }

    public FeatureResult(FeatureResult cf) {
        this.centroid = new Centroid(cf.centroid);
        this.feature = new Feature(cf.feature);
    }
    
    public Centroid getCentroid() {
        return centroid;
    }

    public void setCentroid(Centroid centroid) {
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
        return String.format("%s | %s", centroid, feature);
    }
}
