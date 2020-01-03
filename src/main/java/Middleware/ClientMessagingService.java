package Middleware;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.netty.channel.ConnectTimeoutException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientMessagingService {
    private ManagedMessagingService mms;
    private Address server;

    public ClientMessagingService(Address server, Address address){
        this.server = server;
        mms = new NettyMessagingService(
                "server",
                address,
                new MessagingConfig());
        mms.start();
    }

    public CompletableFuture<byte[]> sendAndReceive(byte[] data, String type){
        return mms.sendAndReceive(server, type, data, Duration.ofSeconds(20))
                .whenComplete((m,t) -> {
                    if(t!=null)
                        System.out.println("Server is unavailable try again later");
                });
    }
}
