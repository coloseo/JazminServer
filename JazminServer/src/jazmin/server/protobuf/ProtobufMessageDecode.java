package jazmin.server.protobuf;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import jazmin.log.Logger;
import jazmin.log.LoggerFactory;
import jazmin.misc.io.NetworkTrafficStat;

import java.util.List;

/**
 * Created by ging on 25/08/2017.
 * JazminServer
 */

public class ProtobufMessageDecode extends ByteToMessageDecoder {

    private static Logger logger= LoggerFactory.get(ProtobufMessageDecode.class);

    private NetworkTrafficStat networkTrafficStat;

    public ProtobufMessageDecode(NetworkTrafficStat networkTrafficStat) {
        this.networkTrafficStat = networkTrafficStat;
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        try {
            if (byteBuf.readableBytes() < 4) {
                return;
            }
            byteBuf.markReaderIndex();
            int id = byteBuf.readInt();
            int length = byteBuf.readInt();
            byte[] data = byteBuf.readBytes(length).array();

            ProtobufMessage message = new ProtobufMessage(id, length, data);
            list.add(message);
            networkTrafficStat.inBound(8 + length);

        } catch (Exception e) {
            logger.catching(e);
        }

    }
}
