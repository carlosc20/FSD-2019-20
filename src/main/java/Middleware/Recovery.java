package Middleware;

import Middleware.CausalOrder.VectorMessage;
import Middleware.Logging.Logger;
import Middleware.TwoPhaseCommit.DistributedObjects.TransactionalMap;
import java.time.Duration;
import java.util.function.Consumer;

public class Recovery {
    private Logger log;

    public Recovery(Logger log){
        this.log = log;
    }

    public void start(Consumer<Object> callback, ServerMessagingService sms, TransactionalMap tmap){
        System.out.println("recovery:start -> Starting recovery");
        log.recover( (msg)->{
            if(msg instanceof VectorMessage)
                sms.causalOrderRecover(msg, callback);
            else
                tmap.transactionalRecover(msg);
        });

        Duration d = Duration.ofSeconds(10);
        System.out.println("recovery:start -> Handlers registered");
        sms.sendAndReceiveForRecovery(d);
        System.out.println("recovery:start -> causalOrderRecovery message sent");
    }
}
