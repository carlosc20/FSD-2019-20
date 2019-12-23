package Middleware;

import Middleware.CausalOrder.VectorMessage;
import Middleware.Logging.Logger;

import java.time.Duration;
import java.util.function.Consumer;

public class Recovery {
    private ServerMessagingService sms;
    private Logger log;

    public Recovery(ServerMessagingService sms, Logger log){
        this.sms = sms;
        this.log = log;
    }

    public void start(Consumer<Object> callback){
        System.out.println("recovery:start -> Starting recovery");
        log.recover( (msg)->{
            if(msg instanceof VectorMessage)
                sms.coh.read(1, sms.encode(msg), callback); //TODO alta martelada
        });
        Duration d = Duration.ofSeconds(15);
        registerHandlers();
        System.out.println("recovery:start -> Handlers registered");
        sms.sendAndReceiveForRecovery("recovery", sms.coh.getVector(), d);
        System.out.println("recovery:start -> recovery messages sent");
    }

    private void registerHandlers(){
        sms.registerOperation("recovery", (a,b)->{
            System.out.println("recovery:handler -> Received request from: " + a);
            boolean state = sms.coh.treatRecoveryRequest(sms.decode(b),
                                msg -> sms.sendOldOperation(a, msg, "text")); //TODO tirar o type de estático
            return sms.encode(state);
        });
    }
}
