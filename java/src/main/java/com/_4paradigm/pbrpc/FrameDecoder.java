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

public class FrameDecoder extends ByteToMessageDecoder{
    private static final Logger logger = LoggerFactory.getLogger(FrameDecoder.class);
    private static final byte[] primaryMagic = new byte[] { 'S', 'O', 'F', 'A' };
    private RpcContext context;
    
    public FrameDecoder(RpcContext context) {
        this.context = context;
    }
    
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        ByteBuf litBuffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        boolean match = matchMagicKey(litBuffer);
        //TODO (wangtaize) handle mismatch
        if (!match) {
        }
        int metaSize = litBuffer.readInt();
        long dataSize = litBuffer.readLong();
        long totalSize = litBuffer.readLong();
        buffer.readerIndex(24);
        //TODO (wangtaize) handle null
        RpcMeta meta = parseMeta(buffer, metaSize);
        MessageContext mc = context.remove(meta.getSequenceId());
        buffer.readerIndex(24 + metaSize);
        if (mc == null) {
            logger.warn("no message context with seq {}", meta.getSequenceId());
            //TODO(wangtaize) add error message
        }else {
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
        byte[] magic = new byte[4];
        litBuffer.readBytes(magic, 0, 4);
        boolean match = Arrays.equals(magic, primaryMagic);
        return match;
    }

}
