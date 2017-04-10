package com._4paradigm.pbrpc;

import java.util.concurrent.CountDownLatch;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import sofa.pbrpc.SofaRpcMeta;
import sofa.pbrpc.SofaRpcMeta.RpcMeta;

public class SyncRpcChannel implements BlockingRpcChannel {
    private AsyncConnection connection;

    public SyncRpcChannel(AsyncConnection connection) {
        this.connection = connection;
    }

    public Message callBlockingMethod(MethodDescriptor method, RpcController controller, Message request,
            Message responsePrototype) throws ServiceException {
        final MessageContext mc = new MessageContext();
        mc.setRequest(request);
        mc.setParser(responsePrototype.getParserForType());
        long id = GlobalSequence.incrementAndGet();
        RpcMeta meta = SofaRpcMeta.RpcMeta.newBuilder().setType(RpcMeta.Type.REQUEST).setMethod(method.getFullName())
                .setSequenceId(id).build();
        mc.setMeta(meta);
        mc.setSeq(id);
        final CountDownLatch countDown = new CountDownLatch(1);
        mc.setClosure(new Closure() {
            public void done() {
                countDown.countDown();
            }
        });
        connection.sendMessage(mc);
        while (countDown.getCount() > 0) {
            try {
                countDown.await();
            } catch (InterruptedException e) {
                // TODO handle error
            }
        }
        return mc.getResponse();
    }

}
