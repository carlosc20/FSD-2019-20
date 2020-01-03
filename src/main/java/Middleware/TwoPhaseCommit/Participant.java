package Middleware.TwoPhaseCommit;

import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;
import io.netty.channel.ConnectTimeoutException;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class Participant {
    private int id;
    private Address manager;
    private ServerMessagingService sms;
    private Logger log;
    private ScheduledExecutorService ses;
    private HashMap<Integer, Boolean> requestsAnswer;
    private int requestNumber;

    public Participant(int id, Address manager, ServerMessagingService sms, Logger log){
        this.id = id;
        this.manager = manager;
        this.sms = sms;
        this.log = log;
        this.ses = Executors.newScheduledThreadPool(1);
        this.requestsAnswer = new HashMap<>();
        this.requestNumber = 0;
    }

    public <T> void startFirstPhase(BiFunction<T,Identifier, Boolean> firstPhaseAnswer){
        sms.registerOperation("firstphase",
                (a,b) -> parseFirstphaseTM(sms.decode(b), firstPhaseAnswer), ses);
    }

    public <T> void startSecondPhase(Function<T,Boolean> secondPhaseAnswer, Consumer<T> commit, BiConsumer<T,Identifier> abort){
        sms.registerOperation("secondphase",
                (a,b) -> parseSecondPhaseTM(sms.decode(b), secondPhaseAnswer, commit, abort), ses);

    }


    public <T> CompletableFuture<Boolean> sendTransaction(T toSend){
        this.requestNumber++;
        TransactionMessage<T> tm = new TransactionMessage<>(requestNumber, toSend);
        System.out.println("Participant.sendTransaction -> starting transaction");
        System.out.println(tm.toString());
        return sendAndReceiveToManager(tm, requestNumber);
    }

    private CompletableFuture<Boolean> sendAndReceiveToManager(TransactionMessage content, int requestNumber){
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        ScheduledFuture<?> scheduledFuture = ses.scheduleAtFixedRate(() ->
                checkCompletion(cf, requestNumber), 6500, 4000, TimeUnit.MILLISECONDS);
        sms.sendAndReceive(manager, "startTransaction", content, Duration.ofSeconds(6), ses)
                .whenComplete((m,t) -> {
                    if(t!=null){
                        if(t instanceof ConnectTimeoutException){
                            System.out.println("Participant.sendAndReceiveToManager -> Service Unavailable");
                            t.printStackTrace();
                            cf.complete(false);
                        }
                        else{
                            System.out.println("Participant.sendAndReceiveToManager -> Request Timeout");
                            t.printStackTrace();
                            //aqui o scheduledFuture vai entrar em ação
                        }
                    }
                    else{
                        //System.out.println("completing future message " + s.decode(m).toString());
                        cf.complete(sms.decode(m));
                        requestsAnswer.remove(requestNumber);
                    } });
        return cf.whenComplete((m,t) -> scheduledFuture.cancel(true));
    }

    private void checkCompletion(CompletableFuture<Boolean> cf, int requestNumber) {
        if(requestsAnswer.containsKey(requestNumber)){
            cf.complete(requestsAnswer.get(requestNumber));
            requestsAnswer.remove(requestNumber);
        }
    }

    private <T> byte[] parseFirstphaseTM(TransactionMessage<T> tm, BiFunction<T,Identifier,Boolean> firstPhaseAnswer){
        Identifier tid = tm.getTransactionId();
        //se existe na estrutura então recebi uma msg repetida. Manager deu reboot
        //obtém resposta prepared/abort
        boolean state = firstPhaseAnswer.apply(tm.getContent(),tid);
        if(state) {
            System.out.println("Participant.parseFirstphaseTM -> prepared to transaction id == " + tm.getTransactionId());
            tm.setPrepared();
        }
        else{
            System.out.println("Participant.parseFirstphaseTM -> aborting transaction id == " + tm.getTransactionId());
            tm.setAborted();
        }
        log.write(tm);
        return sms.encode(tm);
    }

    private <T> byte[] parseSecondPhaseTM(TransactionMessage<T> tm, Function<T,Boolean> isCommited, Consumer<T> commit, BiConsumer<T,Identifier> abort){
        Identifier tid = tm.getTransactionId();
        //só entra se não está commited ou se ocorreu um abort
        //isto bate tbm no caso do username repetido, pq o primeiro que chegue vai estar commited, mas não importa
        if(!isCommited.apply(tm.getContent())){
            System.out.println("Participant.parseSecondPhaseTM -> transaction not yet commited");
            if(tm.isCommited()) {
                System.out.println("Participant.parseSecondPhaseTM -> commiting transaction id == " + tm.getTransactionId());
                commit.accept(tm.getContent());
                requestsAnswer.put(tm.getRequestId(), true);
            }
            else{
                System.out.println("Participant.parseSecondPhaseTM -> aborting transaction id == " + tm.getTransactionId());
                abort.accept(tm.getContent(), tid);
                requestsAnswer.put(tm.getRequestId(), false);
            }
            log.write(tm);
        }
        return sms.encode(tm);
    }

    public <T> void recovery(BiFunction<T,Identifier,Boolean> firstPhaseAnswer, Consumer<T> commit, BiConsumer<T,Identifier> abort, TransactionMessage<T> tm){
        Identifier tid = tm.getTransactionId();
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

