package com.scaleunlimited.flinksources;

import java.io.Serializable;

/**
 * Base class for terminating a source function after all of the output
 * has been generated. This is needed when a source is feeding an iteration,
 * as otherwise once the source terminates, check-pointing no longer works
 * for functions in the iteration.
 *
 */
@SuppressWarnings("serial")
public abstract class SourceTerminator implements Serializable {

    public abstract boolean isTerminated();
    
    public void startingSource() {};
    
    public void stoppingSource() {};
}
