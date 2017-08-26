package jazmin.server.protobuf.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import jazmin.log.Logger;
import jazmin.log.LoggerFactory;
import jazmin.misc.io.IOWorker;
import jazmin.misc.io.NetworkTrafficStat;
import jazmin.server.protobuf.ProtobufMessage;
import jazmin.server.protobuf.ProtobufMessageDecode;
import jazmin.server.protobuf.ProtobufMessageEncode;
import jazmin.server.rpc.RpcException;
import jazmin.util.DumpUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ging on 25/08/2017.
 * JazminServer
 */

public class ProtobufMessageClient {

    private static Logger logger = LoggerFactory.getLogger(ProtobufMessageClient.class);

    private EventLoopGroup group;
    private Bootstrap bootstrap;
    private NetworkTrafficStat networkTrafficStat;

    private Channel channel;
    private Map<Integer,RPCLock> lockMap;
    private int timeout=5000;//5 sec timeout
    private AtomicInteger messageId;

    public ProtobufMessageClient() {
        networkTrafficStat=new NetworkTrafficStat();
        lockMap=new ConcurrentHashMap<>();
        initNettyConnector();
        messageId=new AtomicInteger(1);
    }

    private void initNettyConnector(){
        IOWorker worker=new IOWorker("ProtobufMessageClientIO",1);
        group = new NioEventLoopGroup(1,worker);
        bootstrap = new Bootstrap();
        ProtobufMessageClientHandler clientHandler = new ProtobufMessageClientHandler(this);

        ChannelInitializer<SocketChannel> channelInitializer=
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel sc) throws Exception {
                        sc.pipeline().addLast(
                                new ProtobufMessageEncode(networkTrafficStat),
                                new ProtobufMessageDecode(networkTrafficStat),
                                clientHandler);

                    }
                };
        bootstrap.group(group);
        bootstrap.channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32*1024)
                .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8*1024)
                .handler(channelInitializer);
    }

    public void connect(String host,int port){
        try {
            if(logger.isWarnEnabled()){
                logger.warn("connect message server {}:{}",host,port);
            }
            channel=bootstrap.connect(host, port).sync().channel();
        } catch (Exception e) {
            logger.error("can not connect to server "+host+":"+port,e);
        }
    }

    public void send(int id, byte[] data){
        ProtobufMessage message = new ProtobufMessage(id, data.length, data);
        if(logger.isDebugEnabled()){
            logger.debug(">>>>>>>>\n"+ DumpUtil.dump(message));
        }
        channel.writeAndFlush(message);
    }

    public void messageRecieved(ProtobufMessage rspMessage) {
        logger.debug("<<<<<<<<\n"+DumpUtil.dump(rspMessage));
        RPCLock lock = lockMap.get(messageId.intValue());
        if(lock!=null){
            synchronized (lock) {
                lock.response=rspMessage;
                lock.notifyAll();
            }
        }
    }

    public ProtobufMessage invokeSync(int id, byte[] data){
        RPCLock lock=new RPCLock();
        lock.startTime=System.currentTimeMillis();
        lock.id=messageId.incrementAndGet();
        lockMap.put(lock.id,lock);
        send(id, data);
        synchronized (lock) {
            try {
                while(lock.response==null){
                    lock.wait(timeout);
                    long currentTime=System.currentTimeMillis();
                    if((currentTime-lock.startTime)>timeout){
                        lockMap.remove(lock.id);
                        throw new RpcException(
                                "rpc request:"+lock.id+" timeout,startTime:"+lock.startTime+", id:"+id);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            lockMap.remove(lock.id);
            return lock.response;
        }
    }

    public static class RPCLock{
        public int id;
        public long startTime;
        public ProtobufMessage response;
    }

    public static void main(String[] args) {
        ProtobufMessageClient client = new ProtobufMessageClient();
        client.connect("127.0.0.1", 2001);
        ProtobufMessage responseMessage = client.invokeSync(10001, "hello world".getBytes());
        logger.info(String.format("response: id %d  %s", responseMessage.getId(), new String(responseMessage.getData())));
    }

}
