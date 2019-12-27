package Middleware;

import Middleware.CausalOrder.VectorMessage;
import Middleware.Logging.Logger;
import Middleware.TwoPhaseCommit.TransactionMessage;

import java.time.Duration;
import java.util.HashMap;
import java.util.function.Consumer;

public class Recovery {
    private ServerMessagingService sms;
    private Logger log;

    public Recovery(ServerMessagingService sms, Logger log){
        this.sms = sms;
        this.log = log;
    }

    public HashMap<Integer, TransactionMessage> start(Consumer<Object> callback){
        System.out.println("recovery:start -> Starting recovery");
        HashMap<Integer, TransactionMessage> auxiliar = new HashMap<>();
        log.recover( (msg)->{
            if(msg instanceof VectorMessage)
                sms.coh.recoveryRead(sms.encode(msg), callback);
            else{
                TransactionMessage tm = (TransactionMessage) msg;
                auxiliar.put(tm.getTransactionId(), tm);
            }
        });
        Duration d = Duration.ofSeconds(15);
        registerHandlers();
        System.out.println("recovery:start -> Handlers registered");
        sms.sendAndReceiveForRecovery("recovery", sms.coh.getVector(), d);
        System.out.println("recovery:start -> recovery messages sent");
        return auxiliar;
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
