package jazmin.server.protobuf;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import jazmin.core.Jazmin;
import jazmin.core.Server;
import jazmin.core.thread.Dispatcher;
import jazmin.core.thread.DispatcherCallbackAdapter;
import jazmin.log.Logger;
import jazmin.log.LoggerFactory;
import jazmin.misc.InfoBuilder;
import jazmin.misc.io.IOWorker;
import jazmin.misc.io.NetworkTrafficStat;
import jazmin.server.console.ConsoleServer;
import jazmin.server.msg.*;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ging on 24/08/2017.
 * JazminServer
 */

public class ProtobufServer extends Server {

    private static Logger logger = LoggerFactory.getLogger(ProtobufServer.class);

    static final int DEFAULT_PORT = 2001;
    static final int DEFAULT_IDLE_TIME = 60 * 10;//10 min

    ServerBootstrap tcpNettyServer;
    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    ChannelInitializer<SocketChannel> tcpChannelInitializer;
    NetworkTrafficStat networkTrafficStat;
    IOWorker ioWorker;

    int port;
    int idleTime;


    SessionManager sessionManager;
    ProtobufServerHandler protobufServerHandler;

    private ProtobufInvokeService protobufInvokeService;

    public ProtobufServer() {
        super();
        port = DEFAULT_PORT;
        idleTime = DEFAULT_IDLE_TIME;
        sessionManager = new SessionManager();
        networkTrafficStat = new NetworkTrafficStat();
    }

    public int getSessionCreateTime() {
        return sessionManager.getSessionCreateTime();
    }

    public void setSessionCreateTime(int sessionCreateTime) {
        this.sessionManager.setSessionCreateTime(sessionCreateTime);
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getIdleTime() {
        return idleTime;
    }

    public void setIdleTime(int idleTime) {
        this.idleTime = idleTime;
    }

    public int getMaxSessionCount() {
        return this.sessionManager.getMaxSessionCount();
    }

    public void setMaxSessionCount(int maxSessionCount) {
        this.sessionManager.setMaxSessionCount(maxSessionCount);
    }

    public int getMaxChannelCount() {
        return this.sessionManager.getMaxChannelCount();
    }

    public void setMaxChannelCount(int maxChannelCount) {
        this.sessionManager.setMaxChannelCount(maxChannelCount);
    }

    public List<Session> getSessions() {
        return new ArrayList<>(this.sessionManager.getSessionMap().values());
    }

    public int getSessionCount() {
        return this.sessionManager.getSessionMap().size();
    }

    public List<Channel> getChannels() {
        return new ArrayList<>(this.sessionManager.getChannelMap().values());
    }

    public long getInBoundBytes() {
        return networkTrafficStat.inBoundBytes.longValue();
    }

    public long getOutBoundBytes() {
        return networkTrafficStat.outBoundBytes.longValue();
    }


    public void setSessionLifecycleListener(SessionLifecycleListener l) {
        this.sessionManager.setSessionLifecycleListener(l);
    }

    public SessionLifecycleListener getSessionLifecycleListener() {
        return this.sessionManager.getSessionLifecycleListener();
    }

    public void sessionDisconnected(Session session) {
        this.sessionManager.sessionDisconnected(session);
    }

    public void sessionCreated(Session session) {
        this.sessionManager.sessionCreated(session);
    }

    public void sessionIdle(Session session) {
        this.sessionManager.sessionIdle(session);
    }

    public void sessionKeepAlive(Session session){
       this.sessionManager.sessionKeepAlive(session);
    }

    public Session getSessionByPrincipal(String principal){
       return this.sessionManager.getSessionByPrincipal(principal);
    }

    public Session getSessionById(int id){
        return this.sessionManager.getSessionById(id);
    }


    public void setPrincipal(Session s,String principal,String userAgent){
       this.sessionManager.setPrincipal(s, principal, userAgent);
    }


    public void broadcast(String serviceId,Object payload){
       this.sessionManager.broadcast(serviceId, payload);
    }

    public Channel createChannel(String id){
        return this.sessionManager.createChannel(id);
    }

    public Channel getChannel(String id){
        return this.sessionManager.getChannel(id);
    }

    public int getChannelCount(){
        return this.sessionManager.getChannelCount();
    }

    public void removeChannelInternal(String id){
        this.sessionManager.removeChannelInternal(id);
    }

    public ProtobufInvokeService getProtobufInvokeService() {
        return protobufInvokeService;
    }

    public void setProtobufInvokeService(ProtobufInvokeService protobufInvokeService) {
        this.protobufInvokeService = protobufInvokeService;
    }

    class ProtobufServerChannelInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast("idleStateHandler", new IdleStateHandler(idleTime, idleTime, 0));
            ch.pipeline().addLast("protobufDecoder", new ProtobufMessageDecode(networkTrafficStat));
            ch.pipeline().addLast("protobufEncoder", new ProtobufMessageEncode(networkTrafficStat));
            ch.pipeline().addLast(protobufServerHandler);
        }
    }

    private void initTcpNettyServer(){
        protobufServerHandler = new ProtobufServerHandler(this);
        tcpNettyServer = new ServerBootstrap();
        tcpChannelInitializer = new ProtobufServerChannelInitializer();

        tcpNettyServer.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_RCVBUF, 1024*256)
                .option(ChannelOption.SO_SNDBUF, 1024*256)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(tcpChannelInitializer);
    }


    public void receiveMessage(Session session, ProtobufMessage message) {
        logger.info(String.format("session id:%d -- message: %s", session.getId(), message.toString()));
        invokeService(session, message);
    }

    private void invokeService(Session session, ProtobufMessage message) {
        Context context = new Context(this, session, message, false, false);
        session.processSyncService(false);
        MessageDispatcherCallback messageDispatcherCallback = new MessageDispatcherCallback();
        messageDispatcherCallback.session = session;

        try {
            Method method = ProtobufInvokeService.class.getDeclaredMethod("invokeService", Context.class);
            Jazmin.dispatcher.invokeInPool(
                    "@"+session.getPrincipal()+"#"+message.getId(),
                    protobufInvokeService,
                    method,
                    messageDispatcherCallback,context);

        } catch (NoSuchMethodException e) {
            logger.error("invoke service error:", e);
        }

    }

    static class MessageDispatcherCallback extends DispatcherCallbackAdapter {
        public Session session;

        @Override
        public void end(Object instance, Method method, Object[] args, Object ret, Throwable e) {
            session.processSyncService(false);
            Context context = (Context)args[0];
            if(e != null) {
                logger.error("invoke service error: %s", e);
            }
            context.close(e!=null);
        }
    }

    @Override
    public void init() throws Exception {
        ioWorker=new IOWorker("PBServerIO",Runtime.getRuntime().availableProcessors()*2+1);
        bossGroup = new NioEventLoopGroup(1,ioWorker);
        workerGroup = new NioEventLoopGroup(0,ioWorker);
        initTcpNettyServer();
    }

    @Override
    public void start() throws Exception {
        tcpNettyServer.bind(port).sync();
        this.sessionManager.startSessionChecker();
    }

    @Override
    public void stop() throws Exception {
        setSessionLifecycleListener(null);
        if(bossGroup!=null){
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    public String info() {
        InfoBuilder ib= InfoBuilder.create()
                .section("info")
                .format("%-50s:%-30s\n")
                .print("port", port)
                .print("idleTime", idleTime+" seconds")
                .print("sessionCreateTime", this.sessionManager.getSessionCreateTime()+" seconds")
                .print("maxSessionCount", this.sessionManager.getMaxSessionCount())
                .print("maxChannelCount", this.sessionManager.getChannelCount())
                .print("maxSessionRequestCountPerSecond", this.sessionManager.getMaxSessionRequestCountPerSecond())
                .print("sessionLifecycleListener", this.sessionManager.getSessionLifecycleListener());
        return ib.toString();
    }
}
