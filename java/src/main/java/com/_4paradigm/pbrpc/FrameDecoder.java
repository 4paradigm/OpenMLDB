package com._4paradigm.pbrpc;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import sofa.pbrpc.SofaRpcMeta;
import sofa.pbrpc.SofaRpcMeta.RpcMeta;

public class FrameDecoder extends ByteToMessageDecoder {
    private static final Logger logger = LoggerFactory.getLogger(FrameDecoder.class);
    private static final byte[] primaryMagic = new byte[] { 'S', 'O', 'F', 'A' };
    private RpcContext context;
    private final int maxFrameLength;
    public FrameDecoder(RpcContext context, int maxFrameLength) {
        this.context = context;
        this.maxFrameLength = maxFrameLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        ByteBuf litBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        boolean match = matchMagicKey(litBuffer);
        if (!match) {
            return;
        }
        int metaSize = litBuffer.readInt();
        long dataSize = litBuffer.readLong();
        long totalSize = litBuffer.readLong();
        buffer.readerIndex(24);
        RpcMeta meta = parseMeta(buffer, metaSize);
        MessageContext mc = context.remove(meta.getSequenceId());
        if (mc == null) {
            logger.warn("no message context with seq {}", meta.getSequenceId());
            return;
        }
        if (totalSize > maxFrameLength) {
            mc.setError(101);
            out.add(mc);
        }else {
            mc.setError(0);
            buffer.readerIndex(24 + metaSize);
            Message response = parseData(mc, buffer, dataSize);
            mc.setResponse(response);
            out.add(mc);
        }
        
    }

    private Message parseData(MessageContext mc, ByteBuf buffer, Long size) {
        try {
            Message response = mc.getParser().parseFrom(new ByteBufInputStream(buffer, size.intValue()));
            return response;
        } catch (InvalidProtocolBufferException e) {
            logger.error("fail to parse data", e);
            return null;
        }
    }

    private RpcMeta parseMeta(ByteBuf buffer, int size) {

        try {
            RpcMeta meta = SofaRpcMeta.RpcMeta.parseFrom(new ByteBufInputStream(buffer, size));
            return meta;
        } catch (IOException e) {
            logger.error("fail to parse meta", e);
            return null;
        }
        
    }

    private boolean matchMagicKey(ByteBuf litBuffer) {
        try {
            byte[] magic = new byte[4];
            litBuffer.readBytes(magic, 0, 4);
            boolean match = Arrays.equals(magic, primaryMagic);
            return match;
        }catch (IndexOutOfBoundsException e) {
            return false;
        }
        
    }

    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        
        super.exceptionCaught(ctx, cause);
    }
    

}
