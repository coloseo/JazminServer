package jazmin.server.protobuf;

import jazmin.log.Logger;
import jazmin.log.LoggerFactory;
import jazmin.server.msg.MessageServer;
import jazmin.server.msg.Session;
import jazmin.server.msg.codec.RequestMessage;
import jazmin.server.msg.codec.ResponseMessage;
import jazmin.util.DumpIgnore;

/**
 * Created by ging on 25/08/2017.
 * JazminServer
 */

@DumpIgnore
public class Context {

    private static Logger logger= LoggerFactory.get(Context.class);

    private boolean isFlush;
    private boolean isDisableResponse;
    private boolean isContinuation;
    private Session session;
    private ProtobufMessage responseObject;
    private ProtobufMessage requestMessage;
    private ProtobufServer messageServer;

    public Context(ProtobufServer messageServer,
                   Session session,
                   ProtobufMessage requestMessage,
                   boolean isDisableResponse,
                   boolean isContinuation){
        this.messageServer=messageServer;
        this.session=session;
        this.requestMessage=requestMessage;
        this.isDisableResponse=isDisableResponse;
        this.isContinuation=isContinuation;
        isFlush=false;
    }

    public int getServiceId(){
        return requestMessage.getId();
    }

    public ProtobufServer getMessageServer() {
        return messageServer;
    }

    public ProtobufMessage getResponseObject() {
        return responseObject;
    }

    public ProtobufMessage getRequestMessage() {
        return requestMessage;
    }

    public Session getSession() {
        return session;
    }

    public void ret(ProtobufMessage rsp) {
        this.responseObject = rsp;
    }

    public void flush(boolean error){
        if(isFlush){
            throw new IllegalStateException("context already flushed");
        }
        isFlush=true;
        if(!isDisableResponse&&!error){
            session.sendProtobufMessage(this.responseObject);
        }
    }

    public void close(boolean error){
        if(!isContinuation){
            flush(error);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if(!isFlush){
            logger.fatal("context did not call flush {}",requestMessage);
        }
    }
}
