package Middleware.TwoPhaseCommit;

import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;


public class Participant {
    private int id;
    private ServerMessagingService sms;
    private Address manager;
    private Logger log;

    public Participant(int id, ServerMessagingService sms, Address manager){
        this.id = id;
        this.sms = sms;
        this.manager = manager;
    }

    public CompletableFuture<TransactionMessage> begin(String name){
        TransactionMessage tm = new TransactionMessage(name);
        return sms.sendAndReceive(manager,"begin", tm);
    }

    public CompletableFuture<TransactionMessage> commit(TransactionMessage tm){
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
        sms.<TransactionMessage>registerOperation("secondphase", (tm) -> {
            switch (tm.getType()) {
                case 'c':
                    //TODO logg
                    callback.accept(tm);
                    System.out.println("Logic.Server " + this.id + " Commited");
                    break;
                case 'a':
                    //TODO logg
                    callback.accept(tm);
                    System.out.println("Logic.Server " + this.id + " Aborted");
                    break;
            }
        });
    }
}
