package Middleware;

import Middleware.CausalOrder.CausalOrderHandler;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class ServerMessagingService {
    private ExecutorService e;
    private ManagedMessagingService mms;
    private CausalOrderHandler coh;
    private Serializer s;
    private List<Address> participants;

    public ServerMessagingService(int id, Address address, List<Address> participants){
        this.e = Executors.newFixedThreadPool(1);
        this.mms = new NettyMessagingService(
                "server",
                address,
                new MessagingConfig());
        this.s = new GlobalSerializer().s;
        this.participants = new ArrayList<>();

        int pSize = participants.size();
        //não contém ele próprio
        for(int i = 0; i<pSize; i++){
            if(i==id) continue;
            this.participants.add(participants.get(i));
        }
        this.coh = new CausalOrderHandler(id, pSize, s);
        List<Integer> vector = this.coh.recover();
        //do something with vector if size != 0

    }

    public void start(){
        mms.start();
    }

    public void registerOperation(String type, BiConsumer<Address,byte[]> callback){
        mms.registerHandler(type, callback, e);
    }

    public void registerOperation(String type, BiFunction<Address, byte[], byte[]> callback){
        mms.registerHandler(type, callback, e);
    }

    public void registerOrderedOperation(String name, Consumer<Object> callback){
        mms.registerHandler(name, (a,b) -> {
            coh.read(b, o->{
                callback.accept(s.encode(o));
            });
        },e);
    }

    public CompletableFuture<Void> sendCausalOrderAsyncToCluster(String type, Object content) {
        byte[] toSend = coh.createMsg(content);
        for (Address a : participants)
            mms.sendAsync(a, type, toSend);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> sendAsyncToCluster(String type, Object content) {
        for (Address a : participants)
            mms.sendAsync(a, type, s.encode(content));
        return CompletableFuture.completedFuture(null);
    }

    public <T> byte[] encode(T object){
        return s.encode(object);
    }

    public <T> T decode(byte[] bytes){
        return s.decode(bytes);
    }

    //DEBUG
    public void send(Address address, Object msg, String type){
        mms.sendAsync(address, type, s.encode(msg));
    }

}
