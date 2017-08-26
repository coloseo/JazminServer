package jazmin.server.protobuf;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import jazmin.log.Logger;
import jazmin.log.LoggerFactory;
import jazmin.misc.io.NetworkTrafficStat;

/**
 * Created by ging on 25/08/2017.
 * JazminServer
 */

public class ProtobufMessageEncode extends MessageToByteEncoder<ProtobufMessage> {

    private static Logger logger= LoggerFactory.get(ProtobufMessageEncode.class);

    private NetworkTrafficStat networkTrafficStat;

    public ProtobufMessageEncode(NetworkTrafficStat networkTrafficStat) {
        this.networkTrafficStat = networkTrafficStat;
    }

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, ProtobufMessage protobufMessage, ByteBuf byteBuf) throws Exception {
        try {
            byteBuf.writeInt(protobufMessage.getId());
            byteBuf.writeInt(protobufMessage.getLength());
            byteBuf.writeBytes(protobufMessage.getData());
            networkTrafficStat.outBound(8 + protobufMessage.getLength());
        } catch (Exception e) {
            logger.catching(e);
        }

    }
}
