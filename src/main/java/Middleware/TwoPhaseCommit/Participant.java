package Middleware.TwoPhaseCommit;

import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;


public class Participant {
    private ServerMessagingService sms;
    private Address manager;
    private Logger log;

    public Participant(ServerMessagingService sms, Address manager, Logger log){
        this.sms = sms;
        this.manager = manager;
        this.log = log;
    }

    public CompletableFuture<Integer> begin(String name){
        System.out.println("participant:begin -> begining transaction");
        return sms.sendAndReceive(manager,"begin", "");
    }

    public CompletableFuture<TransactionMessage> commit(TransactionMessage tm){
        System.out.println("participant:commit -> commiting transaction");
        tm.setType('t');
        return sms.sendAndReceive(manager, "commit", tm);
    }

    public void listeningToFirstPhase(Function<TransactionMessage, Integer> checkStatus) {
        sms.<TransactionMessage>registerOperation("firstphase", (tm) -> {
            int state = checkStatus.apply(tm);
            if (state == 1)
                tm.setType('p');
            else
                tm.setType('a');
            return tm;
        });
    }

    public void listeningToSecondPhase(Consumer<TransactionMessage> callback){
        sms.registerOperation("secondphase", callback);
    }
}
