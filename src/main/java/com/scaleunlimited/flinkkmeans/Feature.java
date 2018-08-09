package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Feature implements Serializable {
    private int id;
    private double x;
    private double y;
    private int centroidId;
    private int processCount;
    
    public Feature() {
        this(-1, 0, 0);
    }

    public Feature(double x, double y) {
        this(-1, x, y, -1);
    }

    public Feature(int id, double x, double y) {
        this(id, x, y, -1);
    }

    public Feature(int id, double x, double y, int centroidId) {
        this.id = id;
        this.x = x;
        this.y = y;
        this.centroidId = centroidId;
        this.processCount = 0;
    }

    public Feature(Feature p) {
        this(p.getId(), p.getX(), p.getY(), p.getCentroidId());
        this.processCount = p.processCount;
    }

    public int getId() {
        return id;
    }
    
    public void setId(int id) {
        this.id = id;
    }
    
    public double getX() {
        return x;
    }
    
    public void setX(double x) {
        this.x = x;
    }
    
    public double getY() {
        return y;
    }
    
    public void setY(double y) {
        this.y = y;
    }
    
    public int getCentroidId() {
        return centroidId;
    }

    public void setCentroidId(int centroidId) {
        this.centroidId = centroidId;
    }
    
    public int getProcessCount() {
        return processCount;
    }
    
    public void incProcessCount() {
        processCount += 1;
    }
    
    public void setProcessCount(int processCount) {
        this.processCount = processCount;
    }
    
    public double distance(Feature feature) {
        return Math.sqrt(Math.pow(feature.x - x, 2) + Math.pow(feature.y - y, 2));
    }
    
    public Feature times(int scalar) {
        x *= scalar;
        y *= scalar;
        return this;
    }
    
    public Feature divide(int scalar) {
        x /= scalar;
        y /= scalar;
        return this;
    }
    
    public Feature plus(Feature feature) {
        x += feature.x;
        y += feature.y;
        return this;
    }
    
    public Feature minus(Feature feature) {
        x -= feature.x;
        y -= feature.y;
        return this;
    }
    
    @Override
    public String toString() {
        if (centroidId != -1) {
            return String.format("Feature %d: %f,%f (centroid %s, count %d)", id, x, y, centroidId, processCount);
        } else {
            return String.format("Feature %d: %f,%f (unassigned)", id, x, y);
        }
    }
}