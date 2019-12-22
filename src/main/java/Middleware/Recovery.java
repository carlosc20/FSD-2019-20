package Middleware;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

public class Recovery {
    private ServerMessagingService sms;

    public Recovery(ServerMessagingService sms){
        this.sms = sms;
    }

    public void start(Consumer<Object> callback){
        System.out.println("Starting recovery");
        List<Integer> vector = sms.coh.recover(callback);
        Duration d = Duration.ofSeconds(15);
        registerHandlers();
        System.out.println("Handlers registered");
        sms.sendAndReceiveForRecovery("recovery", vector, d);
        System.out.println("recovery messages sent");
    }

    private void registerHandlers(){
        sms.registerOperation("recovery", (a,b)->{
            boolean state = sms.coh.treatRecoveryRequest(sms.decode(b),
                                msg -> sms.sendOldOperation(a, msg, "text")); //TODO tirar o type de estático
            return sms.encode(state);
        });
    }
}
