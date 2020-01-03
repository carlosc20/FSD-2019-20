package Middleware;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class ClientMessagingService {
    private ManagedMessagingService mms;
    private Address server;

    private static final int TIMEOUT_S = 20;


    public ClientMessagingService(Address server, Address address){
        this.server = server;
        mms = new NettyMessagingService(
                "server",
                address,
                new MessagingConfig());
        mms.start();
    }

    public CompletableFuture<byte[]> sendAndReceive(byte[] data, String type){
        return mms.sendAndReceive(server, type, data, Duration.ofSeconds(TIMEOUT_S)).whenComplete((m,t) -> {
                    if(t!=null) System.out.println("CMS: Server is unavailable");
                });
    }
}
