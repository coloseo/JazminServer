package jazmin.server.protobuf;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import jazmin.log.Logger;
import jazmin.log.LoggerFactory;
import jazmin.server.msg.NettyNetworkChannel;
import jazmin.server.msg.Session;

import java.io.IOException;

/**
 * Created by ging on 24/08/2017.
 * JazminServer
 */

class ProtobufServerHandler extends ChannelHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(ProtobufServerHandler.class);

    private static final AttributeKey<Session> SESSION_KEY = AttributeKey.valueOf("s");

    private ProtobufServer protobufServer;


    public ProtobufServerHandler(ProtobufServer protobufServer) {
        this.protobufServer = protobufServer;
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Session session = ctx.channel().attr(SESSION_KEY).get();
        ProtobufMessage message = (ProtobufMessage)msg;
        this.protobufServer.receiveMessage(session, message);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Session session = ctx.channel().attr(SESSION_KEY).get();
        protobufServer.sessionDisconnected(session);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Session session=new Session(new NettyNetworkChannel(ctx.channel()));
        ctx.channel().attr(SESSION_KEY).set(session);
        protobufServer.sessionCreated(session);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if(evt instanceof IdleStateEvent){
            Session session=ctx.channel().attr(SESSION_KEY).get();
            protobufServer.sessionIdle(session);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if(cause instanceof IOException){
            logger.warn("exception on channal:"+ctx.channel()+","+cause.getMessage());
        }else{
            logger.error("exception on channal:"+ctx.channel(),cause);
        }
        ctx.close();
    }
}
