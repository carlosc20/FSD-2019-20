package Middleware.TwoPhaseCommit;

import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class Participant {
    private int id;
    private Address manager;
    private ServerMessagingService sms;
    private Logger log;
    private HashMap<Integer, Boolean> transactionsState;
    private int maxTransactionId;

    public Participant(int id, Address manager, ServerMessagingService sms, Logger log){
        this.id = id;
        this.maxTransactionId = 0;
        this.manager = manager;
        this.sms = sms;
        this.log = log;
        this.transactionsState = new HashMap<>();
    }

    public <T> void startFirstPhase(Function<T,Boolean> firstPhaseAnswer){
        sms.<TransactionMessage<T>>registerOperation("firstphase", (a,b) -> {
            return parseFirstphaseTM(sms.decode(b), firstPhaseAnswer);});
    }

    public <T> void startSecondPhase(Function<T,Boolean> secondPhaseAnswer, Consumer<T> commit, Consumer<T> abort){
        sms.<TransactionMessage<T>>registerOperation("secondphase",
                (a,b) -> {return parseSecondPhaseTM(sms.decode(b), secondPhaseAnswer, commit, abort);});
    }

    public <T> CompletableFuture<byte[]> sendTransaction(T toSend){
        TransactionMessage<T> tm = new TransactionMessage<>(id, toSend);
        System.out.println("dtm:sendTransaction -> starting transaction");
        sms.sendAndReceiveLoop(manager, "startTransaction", tm, Duration.ofSeconds(6), x ->System.out.println("received"));
        //TODO corrigir se der
        return CompletableFuture.completedFuture(null);
    }

    private <T> byte[] parseFirstphaseTM(TransactionMessage<T> tm, Function<T,Boolean> firstPhaseAnswer){
        int tid = tm.getTransactionId();
        //se existe na estrutura então recebi uma msg repetida. Manager deu reboot
        if(transactionsState.containsKey(tid)){
            System.out.println("p:parseFirstphaseTM -> transaction id == " + tm.getTransactionId() + "repeated request");
           boolean savedState = transactionsState.get(tid);
           if(savedState)
               tm.setPrepared();
           else
               tm.setAborted();
        }
        else{
            boolean state = firstPhaseAnswer.apply(tm.getContent());
            transactionsState.put(tid, state);
            if(state) {
                System.out.println("p:parseFirstphaseTM -> prepared to transaction id == " + tm.getTransactionId());
                tm.setPrepared();
                log.write(tm);
            }
            else{
                System.out.println("p:parseFirstphaseTM -> aborting transaction id == " + tm.getTransactionId());
                tm.setAborted();
                log.write(tm);
            }
        }
        tm.setSenderId(this.id);
        return sms.encode(tm);
    }

    private <T> byte[] parseSecondPhaseTM(TransactionMessage<T> tm, Function<T,Boolean> secondPhaseAnswer, Consumer<T> commit, Consumer<T> abort){
        int tid = tm.getTransactionId();
        transactionsState.remove(tid);
        //se já está commited só manda a confirmação
        //colocar no logg transação concluída ? para não ter de spammar sempre
        if(secondPhaseAnswer.apply(tm.getContent())){
            System.out.println("p:parseSecondPhaseTM -> transaction not yet commited");
            if(tm.isCommited()) {
                System.out.println("p:parseSecondPhaseTM -> commiting transaction id == " + tm.getTransactionId());
                commit.accept(tm.getContent());
            }
            else{
                System.out.println("p:parseSecondPhaseTM -> aborting transaction id == " + tm.getTransactionId());
                abort.accept(tm.getContent());
            }
            log.write(tm);
        }
        tm.setSenderId(this.id);
        return sms.encode(tm);
    }

    public <T> void recovery(Function<T,Boolean> firstPhaseAnswer, Consumer<T> commit, Consumer<T> abort, TransactionMessage<T> tm){
        int tid = tm.getTransactionId();
        maxTransactionId = Integer.max(maxTransactionId, tm.getTransactionId());
        System.out.println(maxTransactionId);
        T content = tm.getContent();
        if(tm.isPrepared() ){
            transactionsState.put(tid, firstPhaseAnswer.apply(content));
        }
        else if(tm.isAborted()){
            abort.accept(content);
            transactionsState.put(tid, false);
        }
        else {
            commit.accept(content);
            transactionsState.put(tid, true);
        }
    }

    public void sendRecoveryRequest(Duration d, Consumer<Object> callback){
        sms.sendAndReceiveLoop(manager,"transactionalRecovery", maxTransactionId+1, d, callback);
    }

}

