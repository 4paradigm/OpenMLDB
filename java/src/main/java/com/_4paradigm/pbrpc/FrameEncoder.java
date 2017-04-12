package com._4paradigm.pbrpc;

import java.nio.ByteOrder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class FrameEncoder extends MessageToByteEncoder<MessageContext> {
    private static final byte[] primaryMagic = new byte[] { 'S', 'O', 'F', 'A' };
    private RpcContext context;
    
    public FrameEncoder(RpcContext context) {
        this.context = context;
    }
    
    @Override
    protected void encode(ChannelHandlerContext ctx, MessageContext mc, ByteBuf out) throws Exception {
        ByteBuf header = out.alloc().directBuffer(24);
        header = header.order(ByteOrder.LITTLE_ENDIAN);
        header.writeBytes(primaryMagic);
        header.writeInt(mc.getMeta().getSerializedSize());
        header.writeLong(mc.getRequest().getSerializedSize());
        header.writeLong(mc.getMeta().getSerializedSize() + mc.getRequest().getSerializedSize());
        out.writeBytes(header);
        header.release();
        mc.getMeta().writeTo(new ByteBufOutputStream(out));
        mc.getRequest().writeTo(new ByteBufOutputStream(out));
        context.put(mc.getSeq(), mc);
        ctx.flush();
    }

}
