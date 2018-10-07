package com.scaleunlimited.flinkkmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ClusterUpdate implements Serializable {

    private Feature _feature;
    private int _fromClusterId;
    private int _toClusterId;
    private boolean _newCluster;
    
    public ClusterUpdate() {
        // So it's a POJO
    }
    
    public ClusterUpdate(Feature feature, int fromClusterId, int toClusterId) {
        this(feature, fromClusterId, toClusterId, false);
    }

    public ClusterUpdate(Feature feature, int fromClusterId, int toClusterId, boolean newCluster) {
        _feature = new Feature(feature);
        _fromClusterId = fromClusterId;
        _toClusterId = toClusterId;
        _newCluster = newCluster;
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
    
    public boolean isNewCluster() {
        return _newCluster;
    }
    
    public void setNewCluster(boolean newCluster) {
        _newCluster = newCluster;
    }
    
    @Override
    public String toString() {
        return String.format("Moving %s from %d to %d", _feature, _fromClusterId, _toClusterId);
    }
    
}
