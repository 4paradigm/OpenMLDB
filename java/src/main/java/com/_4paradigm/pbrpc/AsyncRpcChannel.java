    package com._4paradigm.pbrpc;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;

import sofa.pbrpc.SofaRpcMeta;
import sofa.pbrpc.SofaRpcMeta.RpcMeta;

public class AsyncRpcChannel implements RpcChannel {

    private AsyncConnection connection;

    public AsyncRpcChannel(AsyncConnection connection) {
        this.connection = connection;
    }

    public void callMethod(MethodDescriptor method, final RpcController controller, Message request,
            Message responsePrototype, final RpcCallback<Message> done) {
        final MessageContext mc = new MessageContext();
        mc.setRequest(request);
        mc.setParser(responsePrototype.getParserForType());
        long id = GlobalSequence.incrementAndGet();
        final RpcMeta meta = SofaRpcMeta.RpcMeta.newBuilder().setType(RpcMeta.Type.REQUEST)
                .setMethod(method.getFullName()).setSequenceId(id).build();
        mc.setMeta(meta);
        mc.setSeq(id);
        mc.setClosure(new Closure() {
            public void done() {
                if (mc.getError() > 0) {
                    switch (mc.getError()) {
                    case 101:
                        controller.setFailed(Integer.toString(mc.getError()));
                        break;
                    default:
                        break;
                    }
                }else {
                    done.run(mc.getResponse());
                }
            }
        });
        connection.sendMessage(mc);
    }

}
