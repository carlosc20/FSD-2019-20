package Middleware.TwoPhaseCommit;

import Middleware.GlobalSerializer;
import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.DistributedObjects.MapMessage;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class Manager {
    private int id;
    private int numTransactions;
    private Map<Integer, Integer> transactions;
    private Serializer s;
    private Logger log;
    private ServerMessagingService sms;

    public Manager(int id, Address address, List<Address> participants) {
        this.id = id;
        this.numTransactions= 0;
        this.transactions= new HashMap<>();
        this.s = new GlobalSerializer().build();
        this.log = new Logger("logs", "Manager", s);
        this.sms = new ServerMessagingService(id, address, participants, log, s);
        recover();
        start();
    }

    private void start() {
        sms.registerCompletableOperation("startTransaction", (a, b) -> {
            TransactionMessage tm = s.decode(b);
            beginTransaction(tm);
            System.out.println(tm.toString());
            log.write(tm);
            tm.setPhase(1);
            return startFullProtocol(tm);
        });
    }

    private void beginTransaction(TransactionMessage tm){
        numTransactions++;
        System.out.println("Manager.beginTranscation -> transaction request id " + numTransactions);
        transactions.put(numTransactions, 1);
        Identifier id = new Identifier(this.id, numTransactions);
        tm.setTransactionId(id);
        tm.setPrepared();
    }


    private CompletableFuture<byte[]> startFullProtocol(TransactionMessage tm){
        CompletableFuture<byte[]> reply = new CompletableFuture<>();
        int tid = tm.getTransactionId().getId();
        sms.sendAndReceiveToCluster("firstphase", tm, 6, firstPhase)
                .thenAccept(x -> {
                    boolean state = transactions.get(tid) == 1;
                    if(state)
                        tm.setCommited();
                    else
                        tm.setAborted();
                    log.write(tm);
                    //fase é para participantes e não para o manager
                    tm.setPhase(2);
                    sms.sendAndReceiveToCluster("secondphase", tm, 6)
                            .thenAccept(y -> {
                                tm.setFinished();
                                log.write(tm);
                                if(state)
                                    reply.complete(s.encode(true));
                                else
                                    reply.complete(s.encode(false));
                                //System.out.println("manager:secondphase -> removing entry to tid == " + tid);
                                //acabou -> para responder a quem pede
                                transactions.remove(tid);
                    });
                });
        return reply;
    }

    private CompletableFuture<Void> startHalfProtocol(TransactionMessage tm){
        return sms.sendAndReceiveToCluster("secondphase", tm, 6)
                    .thenAccept(y -> {
                        tm.setFinished();
                        log.write(tm);
                        transactions.put(tm.getTransactionId().getId(),3);
                    });
    }


    private Consumer<Object> firstPhase = (b) ->{
        TransactionMessage tm = (TransactionMessage) b;
        int tid = tm.getTransactionId().getId();
            if (tm.isAborted()) {
                transactions.put(tid, 2);
                System.out.println("Manager.firstPhase -> aborting tid == " + tm.getTransactionId() + " from ");
            }
    };

    private void recover(){
        HashMap<Integer, TransactionMessage> auxiliar = new HashMap<>();
        log.recover((msg) ->{
            TransactionMessage tm = (TransactionMessage) msg;
            auxiliar.put(tm.getTransactionId().getId(), tm);
        });
        for(TransactionMessage tm : auxiliar.values()){
            numTransactions++;
            if(tm.isPrepared()){
                transactions.put(tm.getTransactionId().getId(), 1);
                startFullProtocol(tm);
            }
            else if(!tm.isFinished()) {
                startHalfProtocol(tm);
            }
        }
    }


    //Testes ...........................................................................................................

    public static void main(String[] args) {
        ArrayList<Address> addresses = new ArrayList<>();
        Address manager = Address.from("localhost", 20000);
        for(int i = 0; i<3; i++){
            addresses.add(Address.from("localhost",10000 + i));
        }
        Manager managerServer = new Manager(100, manager, addresses);
        managerServer.start();
    }
}
