package com._4paradigm.pbrpc;

import java.nio.ByteOrder;

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

    private String host;
    private int port;
    private RpcContext context;
    private ChannelFuture channel = null;
    private NioEventLoopGroup group = null;

    public AsyncConnection(String host, int port) {
        this.host = host;
        this.port = port;
        this.context = new RpcContext();
    }

    public void connect() throws InterruptedException {
        group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(group);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) throws Exception {
                // 切出一个完整的frame
                channel.pipeline().addLast("FrameSpliter",
                        new LengthFieldBasedFrameDecoder(ByteOrder.LITTLE_ENDIAN, 1024 * 1024, 16, 8, 0, 0, true));
                // 解码frame, 这里也会做protobuf序列化
                channel.pipeline().addLast("FrameDecoder", new FrameDecoder(context));
                //
                channel.pipeline().addLast("processor", new ClientHandler());
                // 编码frame
                channel.pipeline().addLast("FrameEncoder", new FrameEncoder(context));
            }
        });
        channel = b.connect(host, port).sync();
    }

    public void sendMessage(MessageContext mc) {
        channel.channel().pipeline().writeAndFlush(mc);
    }

    public void close() {
        group.shutdownGracefully();
        channel.cancel(false);
    }
}
