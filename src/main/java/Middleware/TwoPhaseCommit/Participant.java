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
        return sms.sendAndReceive(manager,"controller", tm);
    }

    public CompletableFuture<TransactionMessage> commit(TransactionMessage tm){
        tm.setType('t');
        return sms.sendAndReceive(manager, "2pc", tm);
    }

    public void listeningToTwoPhaseCommit(Function<TransactionMessage, Integer> checkStatus, Consumer<TransactionMessage> callback){
        sms.<TransactionMessage>registerOperation("2pc", (tm) -> {
                switch (tm.getType()) {
                    case 'p':
                        int state = checkStatus.apply(tm);
                        if(state == 1)
                            tm.setType('p');
                        else
                            tm.setType('a');
                        break;
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
