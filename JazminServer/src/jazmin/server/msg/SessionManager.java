package jazmin.server.msg;

import jazmin.core.Jazmin;
import jazmin.core.thread.Dispatcher;
import jazmin.log.Logger;
import jazmin.log.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ging on 25/08/2017.
 * JazminServer
 */

public class SessionManager {

    private static final int DEFAULT_MAX_SESSION_COUNT = 8000;

    private static final int DEFAULT_SESSION_CREATE_TIME = 60;

    private static final int DEFAULT_MAX_CHANNEL_COUNT=1000;

    private static Logger logger = LoggerFactory.getLogger(SessionManager.class);

    private Map<Integer,Session> sessionMap;

    private Map<String,Session> principalMap;

    private Map<String, Channel> channelMap;

    private AtomicInteger sessionId;

    private int maxSessionRequestCountPerSecond;

    private int maxSessionCount;

    private int sessionCreateTime;//session create idle time in seconds

    private int maxChannelCount;;

    private SessionLifecycleListener sessionLifecycleListener;

    private Method sessionCreatedMethod;

    private Method sessionDisconnectedMethod;


    public SessionManager() {
        sessionMap = new ConcurrentHashMap<>();
        principalMap = new ConcurrentHashMap<>();
        channelMap = new ConcurrentHashMap<>();

        maxSessionRequestCountPerSecond = 10;
        maxSessionCount = DEFAULT_MAX_SESSION_COUNT;
        sessionCreateTime = DEFAULT_SESSION_CREATE_TIME;
        maxChannelCount = DEFAULT_MAX_CHANNEL_COUNT;
        sessionId = new AtomicInteger(0);

        sessionCreatedMethod = Dispatcher.getMethod(
                SessionLifecycleListener.class,
                "sessionCreated",Session.class);

        sessionDisconnectedMethod = Dispatcher.getMethod(
                SessionLifecycleListener.class,
                "sessionDisconnected",Session.class);
    }

    public int getMaxSessionRequestCountPerSecond() {
        return maxSessionRequestCountPerSecond;
    }

    public void setMaxSessionRequestCountPerSecond(int maxSessionRequestCountPerSecond) {
        this.maxSessionRequestCountPerSecond = maxSessionRequestCountPerSecond;
    }

    public int getSessionCreateTime() {
        return sessionCreateTime;
    }

    public void setSessionCreateTime(int sessionCreateTime) {
        this.sessionCreateTime = sessionCreateTime;
    }

    public int getMaxSessionCount() {
        return maxSessionCount;
    }

    public void setMaxSessionCount(int maxSessionCount) {
        this.maxSessionCount = maxSessionCount;
    }

    public int getMaxChannelCount() {
        return maxChannelCount;
    }

    public void setMaxChannelCount(int maxChannelCount) {
        this.maxChannelCount = maxChannelCount;
    }

    public Map<Integer, Session> getSessionMap() {
        return sessionMap;
    }

    public void setSessionMap(Map<Integer, Session> sessionMap) {
        this.sessionMap = sessionMap;
    }

    public Map<String, Channel> getChannelMap() {
        return channelMap;
    }

    public void setChannelMap(Map<String, Channel> channelMap) {
        this.channelMap = channelMap;
    }

    public Channel getChannel(String id){
        return channelMap.get(id);
    }

    public void removeChannelInternal(String id){
        if(logger.isDebugEnabled()){
            logger.debug("remove channel:"+id);
        }
        channelMap.remove(id);
    }

    public void startSessionChecker(){
        Jazmin.scheduleAtFixedRate(()->{
            long currentTime = System.currentTimeMillis();
            for(Session session:sessionMap.values()){
                try{
                    checkPrincipal(currentTime,session);
                }catch(Exception e){
                    logger.error(e.getMessage(),e);
                }
            }
        },30,30, TimeUnit.SECONDS);
    }

    private void checkPrincipal(long currentTime,Session session) {
        if (session.getPrincipal() == null) {
            if ((currentTime - session.getCreateTime().getTime())> sessionCreateTime * 1000) {
                session.kick("principal null");
            }
        }
    }

    public SessionLifecycleListener getSessionLifecycleListener() {
        return sessionLifecycleListener;
    }

    public void setSessionLifecycleListener(SessionLifecycleListener sessionLifecycleListener) {
        this.sessionLifecycleListener = sessionLifecycleListener;
    }

    public void sessionDisconnected(Session session){
        synchronized (session) {
            sessionDisconnected0(session);
        }
    }

    private void sessionDisconnected0(Session session){
        if(session.getPrincipal()==null){
            logger.debug("bad session disconnected:"+
                    session.getId()+"/"+
                    session.getRemoteHostAddress()+":"+
                    session.getRemotePort());
            sessionMap.remove(session.getId());
            return;
        }
        if(logger.isDebugEnabled()){
            logger.debug("session disconnected:"+
                    session.getId()+"/"+session.getPrincipal()+"/"+
                    session.getRemoteHostAddress()+":"+
                    session.getRemotePort());
        }
        sessionMap.remove(session.getId());
        //
        if(session.getPrincipal()!=null){
            principalMap.remove(session.getPrincipal());
        }
        //auto remove disconnect session from room
        session.getChannels().forEach(cname->{
            Channel cc = getChannel(cname);
            if(cc!=null){
                if(cc.isAutoRemoveDisconnectedSession()){
                    cc.removeSession(session);
                }
            }
        });
        //fire session disconnect event in thread pool
        if(sessionLifecycleListener!=null){
            Jazmin.dispatcher.invokeInPool(
                    session.getPrincipal(),
                    sessionLifecycleListener,
                    sessionDisconnectedMethod,
                    Dispatcher.EMPTY_CALLBACK,
                    session);
        }
    }

    public void sessionCreated(Session session){
        synchronized (session) {
            sessionCreated0(session);
        }
    }

    private void sessionCreated0(Session session){
        session.setMaxRequestCountPerSecond(maxSessionRequestCountPerSecond);
        if(sessionMap.size()>=maxSessionCount){
            session.kick("too many sessions:"+maxSessionCount);
            return;
        }
        int nextSessionId=sessionId.incrementAndGet();
        session.setId(nextSessionId);
        if(logger.isDebugEnabled()){
            logger.debug("session created:"+
                    session.getId()+"/"+
                    session.getRemoteHostAddress()+":"+
                    session.getRemotePort());
        }
        //
        sessionMap.put(session.getId(),session);
        //fire session create event in thread pool
        if(sessionLifecycleListener!=null){
            Jazmin.dispatcher.invokeInPool(
                    session.getPrincipal(),
                    sessionLifecycleListener,
                    sessionCreatedMethod,
                    Dispatcher.EMPTY_CALLBACK,
                    session);
        }
    }

    public void sessionIdle(Session session){
        synchronized (session) {
            session.kick("user idle last access:"+
                    new Date(session.getLastAccessTime()));
        }
    }

    public void sessionKeepAlive(Session session){
        synchronized (session) {
            session.lastAccess();
        }
    }

    public Session getSessionByPrincipal(String principal){
        if(principal==null){
            throw new IllegalArgumentException("principal can not be null.");
        }
        return principalMap.get(principal);
    }

    public Session getSessionById(int id){
        return sessionMap.get(id);
    }

    public void setPrincipal(Session s,String principal,String userAgent){
        synchronized (s) {
            setPrincipal0(s,principal,userAgent);
        }
    }
    //
    private void setPrincipal0(Session session,String principal,String userAgent){
        if(principal==null||principal.trim().isEmpty()){
            throw new IllegalArgumentException("principal can not be null");
        }
        if(session.principal!=null){
            throw new IllegalStateException("principal already set to:"+
                    session.principal);
        }
        session.setPrincipal(principal);
        session.setUserAgent(userAgent);
        Session oldSession=principalMap.get(principal);
        if(oldSession==session){
            //same session,ignore it
            return ;
        }
        if(oldSession!=null){
            principalMap.remove(principal);
            sessionMap.remove(oldSession.id);
            oldSession.setPrincipal(null);
            oldSession.kick("kicked by same principal.");
            session.setUserObject(oldSession.userObject);
        }
        principalMap.put(principal,session);
    }

    public void broadcast(String serviceId,Object payload){
        sessionMap.forEach((id,session)->{
            session.push(serviceId, payload);
        });
    }

    public Channel createChannel(String id){
        if(logger.isDebugEnabled()){
            logger.debug("create channel:"+id);
        }
        if(id==null){
            throw new IllegalArgumentException("id can not be null.");
        }
        Channel channel=channelMap.get(id);
        if(channel!=null){
            return channel;
        }
        if(channelMap.size()>maxChannelCount){
            throw new IllegalStateException("too many channel,max:"+maxChannelCount);
        }
        channel=new Channel(this,id);
        channelMap.put(id, channel);
        return channel;
    }

    public int getChannelCount(){
        return channelMap.size();
    }


}
