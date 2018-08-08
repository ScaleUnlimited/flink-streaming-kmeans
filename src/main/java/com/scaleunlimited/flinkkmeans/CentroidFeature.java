package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class CentroidFeature implements Serializable {

    private Centroid centroid;
    private Feature feature;
    
    public CentroidFeature(Centroid centroid) {
        this.centroid = new Centroid(centroid);
    }
    
    public CentroidFeature(Feature feature) {
        this.feature = new Feature(feature);
    }
    
    public CentroidFeature(Centroid centroid, Feature feature) {
        if (centroid != null) {
            this.centroid = new Centroid(centroid);
        }
        
        if (feature != null) {
            this.feature = new Feature(feature);
        }
    }

    public CentroidFeature(CentroidFeature cf) {
        this.centroid = new Centroid(cf.centroid);
        this.feature = new Feature(cf.feature);
    }
    
    public Centroid getCentroid() {
        return centroid;
    }

    public void setCentroid(Centroid centroid) {
        this.centroid = centroid;
    }
    
    public Feature getFeature() {
        return feature;
    }
    
    public void setFeature(Feature feature) {
        this.feature = feature;
    }
    
    @Override
    public String toString() {
        if (centroid == null) {
            return feature.toString();
        } else if (feature == null) {
            return centroid.toString();
        } else {
            return String.format("%s | %s", centroid, feature);
        }
    }
}
