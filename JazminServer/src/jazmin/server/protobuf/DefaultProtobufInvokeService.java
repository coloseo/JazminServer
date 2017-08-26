package jazmin.server.protobuf;

import jazmin.log.Logger;
import jazmin.log.LoggerFactory;

/**
 * Created by ging on 25/08/2017.
 * JazminServer
 */

public class DefaultProtobufInvokeService implements ProtobufInvokeService {

    private static Logger logger = LoggerFactory.getLogger(DefaultProtobufInvokeService.class);

    @Override
    public void invokeService(Context context) {
        ProtobufMessage requestMessage = context.getRequestMessage();
        logger.info(String.format("invoke service: id %d length %d data %s", requestMessage.getId(), requestMessage.getLength(), new String(requestMessage.getData())));
        String message = "welcome!";
        ProtobufMessage msg = new ProtobufMessage(10001, message.getBytes().length, message.getBytes());
        context.ret(msg);
    }
}
