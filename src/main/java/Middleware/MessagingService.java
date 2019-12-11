package Middleware;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.util.concurrent.CompletableFuture;

public class MessagingService {

    private ManagedMessagingService mms;
    private Address server;

    public MessagingService(Address server, Address address){
        this.server = server;
        mms = new NettyMessagingService(
                "irrelevante",
                address,
                new MessagingConfig());
    }

    public void start(){
        mms.start();
    }

    public void send(byte[] data, String type){
        mms.sendAsync(server, type, data);
    }

    public CompletableFuture<byte[]> sendAndReceive(byte[] data, String type){
        return mms.sendAndReceive(server, type, data);
    }
}
