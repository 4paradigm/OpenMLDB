package com._4paradigm.pbrpc;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ClientHandler extends SimpleChannelInboundHandler<MessageContext> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, 
                                MessageContext msg) throws Exception {
        msg.finish();
        msg.getClosure().done();
    }

}
