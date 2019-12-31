package Middleware.TwoPhaseCommit;

import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class Participant {
    private int id;
    private Address manager;
    private ServerMessagingService sms;
    private Logger log;

    public Participant(int id, Address manager, ServerMessagingService sms, Logger log){
        this.id = id;
        this.manager = manager;
        this.sms = sms;
        this.log = log;
    }

    public <T> void startFirstPhase(BiFunction<T,Integer, Boolean> firstPhaseAnswer){
        sms.<TransactionMessage<T>>registerOperation("firstphase", (a,b) -> {
            return parseFirstphaseTM(sms.decode(b), firstPhaseAnswer);});
    }

    public <T> void startSecondPhase(Function<T,Boolean> secondPhaseAnswer, Consumer<T> commit, BiConsumer<T,Integer> abort){
        sms.<TransactionMessage<T>>registerOperation("secondphase",
                (a,b) -> {return parseSecondPhaseTM(sms.decode(b), secondPhaseAnswer, commit, abort);});
    }

    public <T> CompletableFuture<byte[]> sendTransaction(T toSend){
        TransactionMessage<T> tm = new TransactionMessage<>(id, toSend);
        System.out.println("dtm:sendTransaction -> starting transaction");
        sms.sendAndReceiveLoop(manager, "startTransaction", tm, 6, x ->System.out.println("received"));
        //TODO corrigir se der
        return CompletableFuture.completedFuture(null);
    }

    private <T> byte[] parseFirstphaseTM(TransactionMessage<T> tm, BiFunction<T,Integer,Boolean> firstPhaseAnswer){
        int tid = tm.getTransactionId();
        //se existe na estrutura então recebi uma msg repetida. Manager deu reboot
        //obtém resposta prepared/abort
        boolean state = firstPhaseAnswer.apply(tm.getContent(),tid);
        if(state) {
            System.out.println("p:parseFirstphaseTM -> prepared to transaction id == " + tm.getTransactionId());
            tm.setPrepared();
        }
        else{
            System.out.println("p:parseFirstphaseTM -> aborting transaction id == " + tm.getTransactionId());
            tm.setAborted();
        }
        log.write(tm);
        tm.setSenderId(this.id);
        return sms.encode(tm);
    }

    private <T> byte[] parseSecondPhaseTM(TransactionMessage<T> tm, Function<T,Boolean> isCommited, Consumer<T> commit, BiConsumer<T,Integer> abort){
        int tid = tm.getTransactionId();
        //só entra se não está commited ou se ocorreu um abort
        //isto bate tbm no caso do username repetido, pq o primeiro que chegue vai estar commited, mas não importa
        if(!isCommited.apply(tm.getContent())){
            System.out.println("p:parseSecondPhaseTM -> transaction not yet commited");
            if(tm.isCommited()) {
                System.out.println("p:parseSecondPhaseTM -> commiting transaction id == " + tm.getTransactionId());
                commit.accept(tm.getContent());
            }
            else{
                System.out.println("p:parseSecondPhaseTM -> aborting transaction id == " + tm.getTransactionId());
                abort.accept(tm.getContent(), tid);
            }
            log.write(tm);
        }
        tm.setSenderId(this.id);
        return sms.encode(tm);
    }

    public <T> void recovery(BiFunction<T,Integer,Boolean> firstPhaseAnswer, Consumer<T> commit, BiConsumer<T,Integer> abort, TransactionMessage<T> tm){
        int tid = tm.getTransactionId();
        T content = tm.getContent();
        if(tm.isPrepared() ){
            firstPhaseAnswer.apply(content,tid);
        }
        else if(tm.isAborted()){
            //se é da segunda faze faço abort
            if(tm.isSecondPhase()){
                abort.accept(content,tid);
            }
        }
        else
            commit.accept(content);
    }
}

