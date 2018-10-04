package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ClusterUpdate implements Serializable {

    private Feature _feature;
    private int _fromClusterId;
    private int _toClusterId;
    
    public ClusterUpdate() {
        // So it's a POJO
    }
    
    public ClusterUpdate(Feature feature, int fromClusterId, int toClusterId) {
        _feature = new Feature(feature);
        _fromClusterId = fromClusterId;
        _toClusterId = toClusterId;
    }

    public Feature getFeature() {
        return _feature;
    }

    public void setFeature(Feature feature) {
        _feature = feature;
    }

    public int getFromClusterId() {
        return _fromClusterId;
    }

    public void setFromClusterId(int fromClusterId) {
        _fromClusterId = fromClusterId;
    }

    public int getToClusterId() {
        return _toClusterId;
    }

    public void setToClusterId(int toClusterId) {
        _toClusterId = toClusterId;
    }
    
    @Override
    public String toString() {
        return String.format("Moving %s from %d to %d", _feature, _fromClusterId, _toClusterId);
    }
    
}
