package com.scaleunlimited.flinkkmeans;

import java.util.Iterator;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class ParallelListSource<T> extends RichParallelSourceFunction<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelListSource.class);

    private List<T> _elements;
    private long _interElementDelay = 0;
    
    private transient int _parallelism;
    private transient int _subtaskIndex;
    private transient volatile boolean _running;
    
    public ParallelListSource(List<T> elements) {
        _elements = elements;
    }
    
    public ParallelListSource(List<T> elements, long interElementDelay) {
        _elements = elements;
        _interElementDelay = interElementDelay;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
        _parallelism = context.getNumberOfParallelSubtasks();
        _subtaskIndex = context.getIndexOfThisSubtask();

    }
    
    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        _running = true;
        
        int index = 0;
        Iterator<T> iter = _elements.iterator();
        while (_running && iter.hasNext()) {
            T element = iter.next();
            if ((index % _parallelism) == _subtaskIndex) {
                LOGGER.debug("Emitting {} at index {} for subtask {}", element.toString(), index, _subtaskIndex);
                ctx.collect(element);
                
                if (_interElementDelay > 0) {
                    Thread.sleep(_interElementDelay);
                }
            }
            
            index += 1;
        }
    }

    @Override
    public void cancel() {
        _running = false;
    }

}
