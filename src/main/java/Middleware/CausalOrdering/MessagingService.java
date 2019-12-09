package Middleware.CausalOrdering;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;


import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MessagingService {

    private ManagedMessagingService mms;
    private Address server;

    public MessagingService(Address server){
        mms = new NettyMessagingService(
                "irrelevante",
                server,
                new MessagingConfig());
    }

    void start(){
        mms.start();
    }

    <T> void send(T content, Serializer s, String type){
        mms.sendAsync(server, type, s.encode(content));
    }

    <T> CompletableFuture<T> sendAndReceive(T content, Serializer s, String type){

        return mms.sendAndReceive(server, type, s.encode(content))
                .thenApply(s::decode);
    }
}
