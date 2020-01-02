package Middleware;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.netty.channel.ConnectTimeoutException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class ClientMessagingService {
    private ManagedMessagingService mms;
    private Address server;

    public ClientMessagingService(Address server, Address address){
        this.server = server;
        mms = new NettyMessagingService(
                "client",
                address,
                new MessagingConfig());
        mms.start();
    }

    public void send(byte[] data, String type){
        mms.sendAsync(server, type, data);
    }

    public CompletableFuture<byte[]> sendAndReceive(byte[] data, String type){
        return mms.sendAndReceive(server, type, data, Duration.ofSeconds(5));
    }
}
