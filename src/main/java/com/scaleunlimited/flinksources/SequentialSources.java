package com.scaleunlimited.flinksources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * A source that will sequentially work its way through a variable number of
 * sources, one at a time. This lets you do things like (a) process a bunch
 * of archived data from a file system, before pulling more recent data from
 * a Kafka topic, or (b) load configuration data into a function before
 * processing other data that is being joined with/processed using this data.
 * 
 * All sources have to output the same type <T>, which means that for the
 * second case you typically need to use a wrapper (e.g. Tuple2<T1, T2>)
 * to effectively "union" two different data types (T1 and T2, here) into
 * a single tuple. One source emits tuples with only T1 filled in, and the
 * other source emits tuples with only T2 filled in.
 * 
 * @param <T>
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
