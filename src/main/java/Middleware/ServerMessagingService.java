package Middleware;

import Middleware.CausalOrder.CausalOrderHandler;
import Middleware.CausalOrder.VectorMessage;
import Middleware.Logging.Logger;

import Middleware.Recovery.MessageRecovery;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ConnectTimeoutException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ServerMessagingService {
    private int id;
    private CausalOrderHandler coh;
    private ScheduledExecutorService ses;
    private ExecutorService e;
    private ManagedMessagingService mms;
    private Serializer s;
    private List<Address> participants;

    public ServerMessagingService(int id, Address address, List<Address> participants, Logger log, Serializer s){
        this.id = id;
        //TODO passar executor para fora?
        this.ses = Executors.newScheduledThreadPool(8);
        this.e = Executors.newFixedThreadPool(1);
        this.mms = new NettyMessagingService(
                "server",
                address,
                new MessagingConfig());
        mms.start();
        this.s = s;
        this.participants = new ArrayList<>();
        int pSize = participants.size();
        for(int i = 0; i<pSize; i++){
            if(i==id) continue; //não contém ele próprio
            this.participants.add(participants.get(i));
        }
        this.coh = new CausalOrderHandler(id, pSize, s, log);
    }

    public void registerOrderedOperation(String name, BiConsumer<Address,Object> callback){
        mms.registerHandler(name, (a,b) -> {
            coh.read(b, o-> callback.accept(a,o));
        },e);
    }

    public void registerCompletableOperation(String type, BiFunction<Address, byte[], CompletableFuture<byte[]>> callback){
        mms.registerHandler(type, callback);
    }

    public void registerOperation(String type, BiFunction<Address, byte[], byte[]> callback){
        mms.registerHandler(type, callback, e);
    }


    public void registerOperation(String type, BiFunction<Address, byte[], byte[]> callback, Executor exec){
        mms.registerHandler(type, callback, exec);
    }

    public void registerOperation(String type, BiConsumer<Address,byte[]> callback){
        mms.registerHandler(type, callback, e);
    }


    //Manager para os servidores
    // sendV2("firstphase", content, senconds, (obj,cf) -> {if first phase ready cf.complete}).thenApply(sendV2("secondPhase",)
    public CompletableFuture<Void> sendAndReceiveToClusterRecovery(String type, Object content, int seconds, BiConsumer<Address,Object> callback){
        //System.out.println("sms:sendAndReceiveLoopToCluster -> type == " + type + content.toString());
        for (Address a : participants){
           sendAndReceiveLoop(a, type, content, seconds)
                    .thenAccept(x -> callback.accept(a, s.decode(x)));
        }
        return CompletableFuture.completedFuture(null);
    }


    //Manager para os servidores
    // sendV2("firstphase", content, senconds, (obj,cf) -> {if first phase ready cf.complete}).thenApply(sendV2("secondPhase",)
    public CompletableFuture<Void> sendAndReceiveToCluster(String type, Object content, int seconds, Consumer<Object> callback){
        //System.out.println("sms:sendAndReceiveLoopToCluster -> type == " + type + content.toString());
        List<CompletableFuture<Void>> requests = new ArrayList<>();
        for (Address a : participants){
            requests.add(sendAndReceiveLoop(a, type, content, seconds)
                    .thenAccept(x -> callback.accept(s.decode(x))));
        }
        return CompletableFuture.allOf(requests.toArray(new CompletableFuture[0]));
    }


    public CompletableFuture<Void> sendAndReceiveToCluster(String type, Object content, int seconds){
        //System.out.println("sms:sendAndReceiveLoopToCluster -> type == " + type + content.toString());
        List<CompletableFuture<byte[]>> requests = new ArrayList<>();
        for (Address a : participants)
            requests.add(sendAndReceiveLoop(a, type, content, seconds));
        return CompletableFuture.allOf(requests.toArray(new CompletableFuture[0]));
    }

    //protótipo
    public CompletableFuture<List<byte[]>> sendAndReceiveToClusterJoined(String type, Object content, int seconds){
        //System.out.println("sms:sendAndReceiveLoopToCluster -> type == " + type + content.toString());
        List<CompletableFuture<byte[]>> requests = new ArrayList<>();
        for (Address a : participants)
            requests.add(sendAndReceiveLoop(a, type, content, seconds));
        return CompletableFuture.allOf(requests.toArray(new CompletableFuture[0]))
                .thenApply(v -> requests.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }

    public CompletableFuture<byte[]> sendAndReceiveLoop(Address a, String type, Object content, int seconds){
        CompletableFuture<byte[]> cf = new CompletableFuture<>();
        ScheduledFuture<?> scheduledFuture = ses.scheduleAtFixedRate(()->
                mms.sendAndReceive(a, type, s.encode(content), e)
                        .whenComplete((m,t) -> {
                            if(t!=null){
                                if(t instanceof ConnectTimeoutException){
                                    System.out.println("server down");
                                }
                                else {
                                    System.out.println("server not responding");
                                }
                            }
                            else{
                                System.out.println("completing future message " + s.decode(m).toString());
                                cf.complete(m);
                            }}), 0, seconds, TimeUnit.SECONDS);

        return cf.whenComplete((m,t) -> scheduledFuture.cancel(true));
    }


    //TODO pq não void?
    public CompletableFuture<Void> sendAsyncToCluster(String type, Object content) {
        //System.out.println("sms:sendAsyncToCluster -> type == " + type);
        for (Address a : participants){
            mms.sendAsync(a, type, s.encode(content));
        }
        return CompletableFuture.completedFuture(null);
    }

    public <T> CompletableFuture<T> sendAndReceive(Address a, String type, Object content){
        return mms.sendAndReceive(a, type, s.encode(content),e)
                    .thenApply(b -> s.decode(b));
    }

    public CompletableFuture<byte[]> sendAndReceive(Address a, String type, Object content, Duration d, ExecutorService e){
        System.out.println(content.toString());
        return mms.sendAndReceive(a, type, s.encode(content), d, e);
    }


    public <T> CompletableFuture<Void> sendAsync(Address a, String type, T content){
        return mms.sendAsync(a,type,s.encode(content));
    }

    public CompletableFuture<Void> sendCausalOrderAsyncToCluster(String type, Object content) {
        //System.out.println("sms:sendCausalOrderAsyncToCluster ->");
        byte[] toSend = coh.createMsg(content);
        for (Address a : participants){
            mms.sendAsync(a, type, toSend);
        }
        return CompletableFuture.completedFuture(null);
    }

    public byte[] getMissingOperationMessage(){
        return s.encode(coh.getMissingOperationMessage());
    }

    public byte[] getVectorMessage(int messageid){
        return s.encode(coh.getVectorMessage(messageid));
    }

    public void causalOrderRecover(Object msg, Consumer<Object> callback){
        coh.recoveryRead(s.encode(msg), callback);
    }

    public void resendMessagesRecover(Object msg, Consumer<Object> callback){
        coh.resendMessagesRead(s.encode(msg), callback);
    }

    public List<Integer> getVector(){
        return coh.getVector();
    }

    public <T> byte[] encode(T object){
        return s.encode(object);
    }

    public <T> T decode(byte[] bytes){
        return s.decode(bytes);
    }

    public int getId(){
        return id;
    }
}
