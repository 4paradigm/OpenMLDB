package com._4paradigm.pbrpc;

import java.util.concurrent.atomic.AtomicLong;

public class GlobalSequence {

    private final static AtomicLong seq = new AtomicLong(0);
    
    public static long incrementAndGet() {
        return seq.getAndIncrement();
    }
    
}
