package jazmin.server.protobuf.client;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import jazmin.log.Logger;
import jazmin.log.LoggerFactory;
import jazmin.server.protobuf.ProtobufMessage;

import java.io.IOException;

/**
 * Created by ging on 25/08/2017.
 * JazminServer
 */

public class ProtobufMessageClientHandler extends ChannelHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(ProtobufMessageClient.class);

    private ProtobufMessageClient protobufMessageClient;


    public ProtobufMessageClientHandler(ProtobufMessageClient protobufMessageClient) {
        this.protobufMessageClient = protobufMessageClient;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if(logger.isInfoEnabled()){
            logger.info("message client  close.{}",ctx.channel());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if(logger.isInfoEnabled()){
            logger.info("message client registered.{}",ctx.channel());
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ProtobufMessage message = (ProtobufMessage)msg;
        protobufMessageClient.messageRecieved(message);
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
