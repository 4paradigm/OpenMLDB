package com._4paradigm.pbrpc;

import java.util.concurrent.ConcurrentHashMap;

public class RpcContext {

    private ConcurrentHashMap<Long, MessageContext> context = new ConcurrentHashMap<Long, MessageContext>();
    
    public boolean put(Long seq, MessageContext mc) {
        if (context.putIfAbsent(seq, mc) == null) {
            return true;
        }
        return false;
    }
    
    public MessageContext remove(Long seq) {
        return context.remove(seq);
    }
}
