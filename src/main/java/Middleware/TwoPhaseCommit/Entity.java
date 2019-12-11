package Middleware.TwoPhaseCommit;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import sun.jvm.hotspot.debugger.Address;

public class Entity {
    private ManagedMessagingService mms;
    private Manager m;
    private Participant p;

/*
    this.sms = new ServersMessagingService(id, myAddr, participants, s);
            this.sms.start();

    public Entity(Address myAddr) {
        this.mms = new NettyMessagingService("irr", myAddr, new MessagingConfig());
        mms.start();
        this.m = new Manager()

    }*/
}
