package com.scaleunlimited.flinkkmeans;

import java.util.List;
import java.util.Random;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

@SuppressWarnings("serial")
public class RandomFeatureSource extends RichSourceFunction<Feature> {

    private int _numFeatures;
    private List<Centroid> _realCentroids;
    private double _featureSpread;
    
    private transient int _subtaskFeatures;
    private transient Random _rand;
    
    private transient volatile boolean _keepRunning;

    public RandomFeatureSource(List<Centroid> realCentroids, int numFeatures, double featureSpread) {
        _realCentroids = realCentroids;
        _numFeatures = numFeatures;
        _featureSpread = featureSpread;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Calculate this subtask's number of features to return, such that the
        // sum for all subtasks will equal our target number of features.
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
        int parallelism = context.getNumberOfParallelSubtasks();
        int subtaskIndex = context.getIndexOfThisSubtask();
        int numFeatures = _numFeatures;
        
        for (int i = 0; i <= subtaskIndex; i++) {
            int subtaskFeatures = numFeatures / parallelism;
            if (i == subtaskIndex) {
                _subtaskFeatures = subtaskFeatures;
            } else {
                numFeatures -= subtaskFeatures;
                parallelism -= 1;
            }
        }
        
        _rand = new Random(subtaskIndex);
    }

    @Override
    public void run(SourceContext<Feature> ctx) throws Exception {
        _keepRunning = true;
        
        while (_keepRunning && (_subtaskFeatures > 0)) {
            ctx.collect(makeRandomFeature());
            
            _subtaskFeatures -= 1;
        }
    }

    /**
     * Create a feature that's some random distance from a random target centroid
     * 
     * @return pseudo-random feature.
     */
    protected Feature makeRandomFeature() {
        Feature f = _realCentroids.get(_rand.nextInt(_realCentroids.size())).getFeature();
        return new Feature(f.getX() + getFeatureDelta(), f.getY() + getFeatureDelta());
    }

    private double getFeatureDelta() {
        // Constrain to a max of 3 standard deviations
        double delta = _rand.nextGaussian();
        delta = Math.min(delta, 3.0);
        delta = Math.max(delta, -3.0);
        
        return _featureSpread * delta;
    }

    @Override
    public void cancel() {
        _keepRunning = false;
    }

}
