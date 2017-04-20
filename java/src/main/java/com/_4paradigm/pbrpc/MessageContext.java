package com._4paradigm.pbrpc;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import sofa.pbrpc.SofaRpcMeta.RpcMeta;

public class MessageContext {

    private long seq;
    private RpcMeta meta;
    private Message request;
    private Message response;
    private Closure closure;
    private Parser<? extends Message> parser;
    private AtomicBoolean done = new AtomicBoolean(false);

    public void finish() {
        done.set(true);
    }

    public Parser<? extends Message> getParser() {
        return parser;
    }

    public RpcMeta getMeta() {
        return meta;
    }

    public void setMeta(RpcMeta meta) {
        this.meta = meta;
    }

    public void setParser(Parser<? extends Message> parser) {
        this.parser = parser;
    }

    public Closure getClosure() {
        return closure;
    }

    public void setClosure(Closure closure) {
        this.closure = closure;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public Message getRequest() {
        return request;
    }

    public void setRequest(Message request) {
        this.request = request;
    }

    public Message getResponse() {
        return response;
    }

    public void setResponse(Message response) {
        this.response = response;
    }
}
