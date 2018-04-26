package com.scaleunlimited.flinkkmeans;

public class Feature {
    private int id;
    private long time;
    private int processCount;
    private double x;
    private double y;
    private int centroidId;
    private int targetCentroidId;
    
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
        this(id, x, y, centroidId, -1);
    }

    public Feature(int id, double x, double y, int centroidId, int targetCentroidId) {
        this.id = id;
        this.time = System.currentTimeMillis();
        this.x = x;
        this.y = y;
        this.centroidId = centroidId;
        this.targetCentroidId = targetCentroidId;
        this.processCount = 0;
    }

    public Feature(Feature p) {
        this(p.getId(), p.getX(), p.getY(), p.getCentroidId());
        this.time = p.getTime();
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
    
    public int getTargetCentroidId() {
        return targetCentroidId;
    }
    
    public void setTargetCentroidId(int targetCentroidId) {
        this.targetCentroidId = targetCentroidId;
    }
    
    public long getTime() {
        return time;
    }
    
    public void setTime(long time) {
        this.time = time;
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
            return String.format("%f,%f (%s)", x, y, centroidId);
        } else {
            return String.format("%f,%f", x, y);
        }
    }
}