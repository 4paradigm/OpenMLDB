package com._4paradigm.pbrpc;

import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class AsyncConnection {
    private final static Logger logger = LoggerFactory.getLogger(AsyncConnection.class);
    private final static int DEFAULT_EVENT_LOOP_THREAD_CNT = 4;
    private String host;
    private int port;
    private RpcContext context;
    private ChannelFuture channel = null;
    private NioEventLoopGroup group = null;
    private int eventLoopThreadCnt;
    public AsyncConnection(String host, int port) {
        this.host = host;
        this.port = port;
        this.context = new RpcContext();
        this.eventLoopThreadCnt = DEFAULT_EVENT_LOOP_THREAD_CNT;
    }

    public void connect() throws InterruptedException {
        group = new NioEventLoopGroup(eventLoopThreadCnt);
        Bootstrap b = new Bootstrap();
        b.group(group);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                channel.pipeline().addLast("FrameSpliter",
                        new LengthFieldBasedFrameDecoder(ByteOrder.LITTLE_ENDIAN, 1024 * 1024, 16, 8, 0, 0, true));
                channel.pipeline().addLast("FrameDecoder", new FrameDecoder(context));
                channel.pipeline().addLast("Processor", new ClientHandler());
                channel.pipeline().addLast("FrameEncoder", new FrameEncoder(context));
            }
        });
        channel = b.connect(host, port).sync();
        logger.info("create a new connection with loop thread cnt {}", eventLoopThreadCnt);
    }

    public void sendMessage(MessageContext mc) {
        channel.channel().pipeline().writeAndFlush(mc);
    }

    public void close() {
        // stop receiving message
        group.shutdownGracefully();
        channel.channel().close();
    }
}
