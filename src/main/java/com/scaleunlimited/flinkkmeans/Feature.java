package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Feature implements Serializable {
    
    // Special ID for a feature that we construct ourselves, vs. coming
    // from data where we have (or can assign) a unique id.
    public static final int NO_FEATURE_ID = -1;
    
    private int id;
    private long timestamp;
    private double x;
    private double y;
    private int clusterId;
    private int processCount;
    
    public Feature() {
        this(0, 0);
    }

    public Feature(double x, double y) {
        this(NO_FEATURE_ID, x, y, Cluster.NO_CLUSTER_ID);
    }

    public Feature(int id, double x, double y) {
        this(id, x, y, Cluster.NO_CLUSTER_ID);
    }

    public Feature(int id, double x, double y, int clusterId) {
        this.id = id;
        this.x = x;
        this.y = y;
        this.clusterId = clusterId;
        this.processCount = 0;
        this.timestamp = System.currentTimeMillis();
    }

    public Feature(Feature p) {
        this(p.getId(), p.getX(), p.getY(), p.getClusterId());
        this.processCount = p.processCount;
        this.timestamp = p.timestamp;
    }

    public Feature(Feature p, int scalar) {
        this(p.getId(), p.getX() / scalar, p.getY() / scalar, p.getClusterId());
        this.processCount = p.processCount;
        this.timestamp = p.timestamp;
    }

    public int getId() {
        return id;
    }
    
    public void setId(int id) {
        this.id = id;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
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
    
    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
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
        return Math.sqrt(Math.pow(x - feature.x, 2) + Math.pow(y - feature.y, 2));
    }
    
    public double distance(Feature feature, int scalar) {
        return Math.sqrt(Math.pow(x / scalar - feature.x, 2) + Math.pow(y / scalar - feature.y, 2));
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
        if (clusterId != Cluster.NO_CLUSTER_ID) {
            return String.format("Feature %d: %f,%f (cluster %s, count %d)", id, x, y, clusterId, processCount);
        } else {
            return String.format("Feature %d: %f,%f (unassigned to cluster)", id, x, y);
        }
    }
}