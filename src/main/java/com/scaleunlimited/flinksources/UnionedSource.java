package com.scaleunlimited.flinksources;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A source that will sequentially work its way through two sources, one at a time. 
 * This lets you do things like  load configuration data into a function before
 * processing other data that is being joined with/processed using this data.
 *
 * @param <T>
 */
@SuppressWarnings("serial")
public class UnionedSource<T0, T1> extends RichParallelSourceFunction<Tuple2<T0, T1>> implements ResultTypeQueryable<Tuple2<T0, T1>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnionedSource.class);

    private TypeInformation<Tuple2<T0, T1>> type;
    private SourceFunction<T0> source0;
    private SourceFunction<T1> source1;
    
    private volatile transient boolean isRunning;
    private transient int sourceId;
    
    public UnionedSource(TypeInformation<Tuple2<T0, T1>> type, SourceFunction<T0> source0, SourceFunction<T1> source1) {
        this.type = type;
        this.source0 = source0;
        this.source1 = source1;
    }
    
    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
        
        if (source0 instanceof RichSourceFunction) {
            ((RichSourceFunction<T0>)source0).setRuntimeContext(t);
        }
        
        if (source1 instanceof RichSourceFunction) {
            ((RichSourceFunction<T1>)source1).setRuntimeContext(t);
        }
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        if (source0 instanceof RichSourceFunction) {
            ((RichSourceFunction<T0>)source0).open(parameters);
        }
        
        if (source1 instanceof RichSourceFunction) {
            ((RichSourceFunction<T1>)source1).open(parameters);
        }
    }
    
    @Override
    public void run(final SourceContext<Tuple2<T0, T1>> context) throws Exception {
        isRunning = true;
        sourceId = 0;
        
        SourceContext<T0> s0ctx = new SourceContext<T0>() {

            @Override
            public void collect(T0 element) {
                context.collect(new Tuple2<T0, T1>(element, null));
            }

            @Override
            public void collectWithTimestamp(T0 element, long timestamp) {
                context.collectWithTimestamp(new Tuple2<T0, T1>(element, null), timestamp);
            }

            @Override
            public void emitWatermark(Watermark mark) {
                context.emitWatermark(mark);
            }

            @Override
            public void markAsTemporarilyIdle() {
                context.markAsTemporarilyIdle();
            }

            @Override
            public Object getCheckpointLock() {
                return context.getCheckpointLock();
            }

            @Override
            public void close() {
                // Ignore, as closing source1 is when we actually want to close
            }
        };
        
        SourceContext<T1> s1ctx = new SourceContext<T1>() {

            @Override
            public void collect(T1 element) {
                context.collect(new Tuple2<T0, T1>(null, element));
            }

            @Override
            public void collectWithTimestamp(T1 element, long timestamp) {
                context.collectWithTimestamp(new Tuple2<T0, T1>(null, element), timestamp);
            }

            @Override
            public void emitWatermark(Watermark mark) {
                context.emitWatermark(mark);
            }

            @Override
            public void markAsTemporarilyIdle() {
                context.markAsTemporarilyIdle();
            }

            @Override
            public Object getCheckpointLock() {
                return context.getCheckpointLock();
            }

            @Override
            public void close() {
                context.close();
            }
            
        };
        
        while (isRunning) {
            if (sourceId == 0) {
                LOGGER.info("Running source 0");
                source0.run(s0ctx);
                sourceId += 1;
                Thread.sleep(1000L);
            } else if (sourceId == 1) {
                LOGGER.info("Running source 1");
                source1.run(s1ctx);
                sourceId += 1;
            } else {
                Thread.sleep(5000L);
                isRunning = false;
            }
        }
        
        LOGGER.info("Done with both sources");
    }

    @Override
    public void cancel() {
        isRunning = false;
        
        // Cancel the active source (if any).
        if (sourceId == 0) {
            source0.cancel();
        } else if (sourceId == 1) {
            source1.cancel();
        }
    }

    @Override
    public TypeInformation<Tuple2<T0, T1>> getProducedType() {
        return type;
    }
}
