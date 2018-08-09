package com.scaleunlimited.flinksources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * A source that will sequentially work its way through a variable number of
 * sources, one at a time. This lets you do things like process a bunch
 * of archived data from a file system, before pulling more recent data from
 * a Kafka topic.
 * 
 * If you want to load configuration data into a function before processing
 * other data that is being joined with/processed using this data, take a
 * look at UnionedSources
 * 
 * @param <T> Type of the source.
 */
@SuppressWarnings("serial")
public class SequentialSources<T> implements SourceFunction<T>, ResultTypeQueryable<T> {

    private TypeInformation<T> type;
    private SourceFunction<T>[] sources;
    
    private volatile transient boolean isRunning;
    private volatile transient int sourceIndex;
    
    @SafeVarargs
    public SequentialSources(TypeInformation<T> type, SourceFunction<T>...sources) {
        this.type = type;
        this.sources = sources;
    }
    
    // TODO - support setRuntimeContext() & open() ala UnionedSources
    
    @Override
    public void run(SourceContext<T> context) throws Exception {
        isRunning = true;
        sourceIndex = -1;
        
        while (isRunning && (sourceIndex < sources.length - 1)) {
            sourceIndex += 1;
            sources[sourceIndex].run(context);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        
        // Cancel the active source (if any).
        if ((sourceIndex >= 0) && (sourceIndex < sources.length)) {
            sources[sourceIndex].cancel();
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return type;
    }
}
