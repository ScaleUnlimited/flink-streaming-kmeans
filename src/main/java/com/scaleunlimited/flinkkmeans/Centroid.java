package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Centroid implements Serializable {

    private Feature feature;
    private int id;
    private CentroidType type;
    private int numFeatures;
    
    public Centroid() {
        // So it's a valid POJO
    }

    public Centroid(Centroid c) {
        this(c.feature, c.id, c.type);
        
        this.numFeatures = c.numFeatures;
    }

    public Centroid(Feature f, int centroidId, CentroidType type) {
        this.feature = new Feature(f);
        this.feature.setId(-1);
        
        this.id = centroidId;
        this.type = type;
        this.numFeatures = 1;
    }

    public int getId() {
        return id;
    }
    
    public void setId(int id) {
        this.id = id;
    }

    public CentroidType getType() {
        return type;
    }
    
    public void setType(CentroidType type) {
        this.type = type;
    }

    public double distance(Feature f) {
        return feature.distance(f);
    }
    
    public void addFeature(Feature f) {
        feature.times(numFeatures).plus(f).divide(numFeatures + 1);
        numFeatures += 1;
    }

    public void removeFeature(Feature f) {
        if (numFeatures > 1) {
            feature.times(numFeatures).minus(f).divide(numFeatures - 1);
            numFeatures -= 1;
        } else {
            throw new RuntimeException(String.format("Centroid %d can't have 0 features!", id));
        }
    }

    public Feature getFeature() {
        return feature;
    }
    
    public void setFeature(Feature feature) {
        this.feature = new Feature(feature);
        this.feature.setId(-1);
    }
    
    public int getNumFeatures() {
        return numFeatures;
    }

    public void setNumFeatures(int numFeatures) {
        this.numFeatures = numFeatures;
    }

    @Override
    public String toString() {
        return String.format("Centroid %d (%s) %s with %d features", id, type, feature.toString(), numFeatures);
    }
}