package com.scaleunlimited.flinkkmeans;

public class Centroid {

    private CentroidType type;
    private long time;
    private Feature feature;
    private int id;
    private int numFeatures;
    
    public Centroid() {
    }

    public Centroid(Feature f, int centroidId, CentroidType type) {
        this.time = System.currentTimeMillis();
        this.type = type;
        this.feature = f;
        this.id = centroidId;
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
        this.feature = feature;
    }
    
    public int getNumFeatures() {
        return numFeatures;
    }

    public void setNumFeatures(int numFeatures) {
        this.numFeatures = numFeatures;
    }

    public long getTime() {
        return time;
    }
    
    public void setTime(long time) {
        this.time = time;
    }
    
    @Override
    public String toString() {
        return String.format("%d (%s) %s with %d features", id, type, feature.toString(), numFeatures);
    }
}