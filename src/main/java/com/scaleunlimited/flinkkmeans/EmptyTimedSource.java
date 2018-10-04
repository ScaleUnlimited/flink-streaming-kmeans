package com.scaleunlimited.flinkkmeans;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.scaleunlimited.flinksources.SourceTerminator;

@SuppressWarnings("serial")
public class EmptyTimedSource<T> extends RichParallelSourceFunction<T> {

    private static final long COLLECTOR_DELAY = 1L;

    private SourceTerminator _terminator;
    
    private transient volatile boolean _running;
    
    public EmptyTimedSource(SourceTerminator terminator) {
        _terminator = terminator;
    }
    
    public EmptyTimedSource() {
        _terminator = new SourceTerminator() {
            
            @Override
            public boolean isTerminated() {
                return false;
            }
        };
    }
    
    @Override
    public void cancel() {
        _running = false;
    }

    @Override
    public void run(SourceContext<T> context) throws Exception {
        _terminator.startingSource();
        
        while (_running && !_terminator.isTerminated()) {
            try {
                // Sleep so we can be interrupted
                Thread.sleep(COLLECTOR_DELAY);
            } catch (InterruptedException e) {
                _running = false;
            }
        }
        
        _terminator.stoppingSource();
    }

}
