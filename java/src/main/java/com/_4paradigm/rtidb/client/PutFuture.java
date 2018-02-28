package com._4paradigm.rtidb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.tablet.Tablet;

public class PutFuture implements Future<Boolean>{

	private List<Future<Tablet.PutResponse>> bf = new ArrayList<Future<Tablet.PutResponse>>();
	private Long startTime = -1l;
	public PutFuture(Future<Tablet.PutResponse> f) {
	    bf.add(f);
	}
	
	public PutFuture(Future<Tablet.PutResponse> f, Long startTime) {
		bf.add(f);
		this.startTime = startTime;
	}
	
	public PutFuture(List<Future<Tablet.PutResponse>> bf) {
	    this.bf = bf;
	}
	
	public static PutFuture wrapper(Future<Tablet.PutResponse> f) {
		return new PutFuture(f);
	}
	
	public static PutFuture wrapper(Future<Tablet.PutResponse> f, Long startTime) {
		return new PutFuture(f, startTime);
	}
	
	public static PutFuture wrapper(List<Future<Tablet.PutResponse>> bf) {
        return new PutFuture(bf);
    }
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
	    boolean ok = true;
	    for (Future<Tablet.PutResponse> f : bf) {
            ok = ok && f.cancel(mayInterruptIfRunning);
        }
	    return ok;
	}

	@Override
	public boolean isCancelled() {
		boolean isCancelled = true;
		for (Future<Tablet.PutResponse> f : bf) {
		    isCancelled = isCancelled && f.isCancelled();
        }
        return isCancelled;
	}

	@Override
	public boolean isDone() {
	    boolean done = true;
	    for (Future<Tablet.PutResponse> f : bf) {
	        done = done && f.isDone();
	    }
	    return done;
	}

	@Override
	public Boolean get() throws InterruptedException, ExecutionException {
	    boolean ok = true;
	    for (Future<Tablet.PutResponse> f : bf) {
            ok = ok && f.get()!= null && f.get().getCode() == 0;
        }
		if (startTime > 0) {
			Long network = System.nanoTime() - startTime;
			if(RTIDBClientConfig.isMetricsEnabled()) {
				TabletMetrics.getInstance().addPut(-1l, network);
			}
		}
		return ok;
	}

	@Override
	public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		throw new ExecutionException("no implementation", null);
	}

}
