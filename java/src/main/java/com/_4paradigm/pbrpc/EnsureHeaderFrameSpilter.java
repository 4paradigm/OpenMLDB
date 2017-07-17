package com._4paradigm.pbrpc;

import java.nio.ByteOrder;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class EnsureHeaderFrameSpilter extends ByteToMessageDecoder {
    private final static Logger logger = LoggerFactory.getLogger(EnsureHeaderFrameSpilter.class);
    private final ByteOrder byteOrder;
    private final int lengthFieldOffset;
    private final int lengthFieldLength;
    private final int maxFrameSize;
    private final int metaFieldOffset;
    private final int metaFieldLength;
    private long byteToDiscard; 
    public EnsureHeaderFrameSpilter(ByteOrder order,
            int metaFieldOffset,
            int metaFieldLength,
            int lengthFieldOffset,
            int lengthFieldLength,
            int maxFrameSize) {
        this.byteOrder = order;
        this.metaFieldOffset = metaFieldOffset;
        this.metaFieldLength = metaFieldLength;
        this.lengthFieldOffset = lengthFieldOffset;
        this.lengthFieldLength = lengthFieldLength;
        this.maxFrameSize = maxFrameSize;
        this.byteToDiscard = 0;
    }
    
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (byteToDiscard > 0) {
            long byteToDiscard = this.byteToDiscard;
            int localBytesToDiscard = (int) Math.min(byteToDiscard, in.readableBytes());
            in.skipBytes(localBytesToDiscard);
            logger.warn("{} bytes to discard", localBytesToDiscard);
            return;
        }
        // the current bytebuf read index
        int rindex = in.readerIndex();
        // goto lengthsize
        int actualLengthFieldOffset = rindex + lengthFieldOffset;
        long frameLength = getFrameLength(in, lengthFieldLength, actualLengthFieldOffset) + lengthFieldOffset + lengthFieldLength;
        // skip max frame size
        if (frameLength > maxFrameSize) {
            int actualMetaLengthFieldOffset = rindex + metaFieldOffset;
            int metaLength = (int)getFrameLength(in, metaFieldLength, actualMetaLengthFieldOffset) + lengthFieldOffset + lengthFieldLength;
            if (in.readableBytes() < metaLength) {
                return;
            }
            ByteBuf data = extractFrame(ctx, in, rindex, metaLength);
            long skipBytes = frameLength - metaLength;
            byteToDiscard = skipBytes - in.readableBytes();
            logger.warn("reache the max frame and discard {} bytes", in.readableBytes());
            in.skipBytes(in.readableBytes());
            out.add(data);
            return;
        }
        int frameLengthInt = (int) frameLength;
        if (in.readableBytes() < frameLengthInt) {
            return;
        }
        ByteBuf data = extractFrame(ctx, in, rindex, frameLengthInt);
        out.add(data);
    }
    
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        ByteBuf frame = ctx.alloc().buffer(length);
        frame.writeBytes(buffer, index, length);
        return frame;
    }
    
    private long getFrameLength(ByteBuf in, int length, int actualLengthFieldOffset) {
        in = in.order(byteOrder);
        long frameLength;
        switch (length) {
        case 1:
            frameLength = in.getUnsignedByte(actualLengthFieldOffset);
            break;
        case 2:
            frameLength = in.getUnsignedShort(actualLengthFieldOffset);
            break;
        case 3:
            frameLength = in.getUnsignedMedium(actualLengthFieldOffset);
            break;
        case 4:
            frameLength = in.getUnsignedInt(actualLengthFieldOffset);
            break;
        case 8:
            frameLength = in.getLong(actualLengthFieldOffset);
            break;
        default:
            throw new Error("should not reach here");
        }
        return frameLength;
    }

}
