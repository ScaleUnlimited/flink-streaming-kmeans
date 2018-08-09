package com.scaleunlimited.flinksources;

public class TimedTerminator extends SourceTerminator {

    private long delay;
    
    private transient long stopTime;
    
    public TimedTerminator(long delay) {
        super();
        
        this.delay = delay;
    }
    
    @Override
    public void stoppingSource() {
        super.stoppingSource();
        
        stopTime = System.currentTimeMillis();
    }
    
    @Override
    public boolean isTerminated() {
        return System.currentTimeMillis() > stopTime + delay;
    }
}
