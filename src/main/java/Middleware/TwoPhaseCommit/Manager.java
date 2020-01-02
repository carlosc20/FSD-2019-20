package Middleware.TwoPhaseCommit;

import Logic.User;
import Middleware.GlobalSerializer;
import Middleware.Logging.Logger;
import Middleware.Marshalling.MessageAuth;
import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.DistributedObjects.MapMessage;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class Manager {
    private int debugCount = 0;
    private int numTransactions;
    private Map<Integer, Integer> transactions;
    private List<Address> staticParticipants;
    private Serializer s;
    private Logger log;
    private ServerMessagingService sms;

    public Manager(int id, Address address, List<Address> participants) {
        this.numTransactions= 0;
        this.transactions= new HashMap<>();
        this.s = new GlobalSerializer().build();
        this.log = new Logger("logs", "Manager", s);
        this.sms = new ServerMessagingService(id, address, participants, log, s);
        this.staticParticipants = participants;
        sms.start();
        recover();
    }

    public void start() {
        sms.registerCompletableOperation("startTransaction", (a, b) -> {
            TransactionMessage tm = sms.decode(b);
            beginTransaction(tm);
            System.out.println(tm.toString());
            log.write(tm);
            tm.setPhase(1);
            return startFullProtocol(tm);
        });
    }

    private void beginTransaction(TransactionMessage tm){
        numTransactions++;
        System.out.println("manager -> transaction request id " + numTransactions);
        transactions.put(numTransactions, 1);
        tm.setTransactionId(numTransactions);
        tm.setPrepared();
    }

    public CompletableFuture<byte[]> debbuger(){
        MapMessage<String, String> mm = new MapMessage<>("marco", "123");
        TransactionMessage tm = new TransactionMessage(0, mm);
        beginTransaction(tm);
        log.write(tm);
        tm.setPhase(1);
        return startFullProtocol(tm);
    }

    private CompletableFuture<byte[]> startFullProtocol(TransactionMessage tm){
        CompletableFuture<byte[]> reply = new CompletableFuture<>();
        int tid = tm.getTransactionId();
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
                                    reply.complete(sms.encode(0));
                                else
                                    reply.complete(sms.encode(1));
                                //System.out.println("manager:secondphase -> removing entry to tid == " + tid);
                                //acabou -> para responder a quem pede
                                transactions.remove(tid);
                    });
                });
        return reply;
    }

    public CompletableFuture<TransactionMessage> debbugerPrototipe(){
        MapMessage<String, String> mm = new MapMessage<>("marco", "123");
        TransactionMessage tm = new TransactionMessage(0, mm);
        beginTransaction(tm);
        log.write(tm);
        tm.setPhase(1);
        return startFullProtocolPrototipe(tm);
    }

    private CompletableFuture<TransactionMessage> startFullProtocolPrototipe(TransactionMessage tm){
        int tid = tm.getTransactionId();
        CompletableFuture<TransactionMessage> cf = new CompletableFuture<>();
        sms.sendAndReceiveToClusterProto("firstphase", tm, 6)
                .thenAccept(list -> {
                    tm.setCommited();
                    for(byte[] b : list){
                        TransactionMessage reply = sms.decode(b);
                        if(reply.isAborted()){
                            tm.setAborted();
                        }
                    }
                    log.write(tm);
                    tm.setPhase(2);
                    sms.sendAndReceiveToCluster("secondphase", tm, 6)
                        .thenAccept(x -> {
                            tm.setFinished();
                            log.write(tm);
                            cf.complete(tm);
                        });
                });
        return cf;
    }

    private CompletableFuture<Void> startHalfProtocol(TransactionMessage tm){
        return sms.sendAndReceiveToCluster("secondphase", tm, 6)
                    .thenAccept(y -> {
                        tm.setFinished();
                        log.write(tm);
                        transactions.put(tm.getTransactionId(),3);
                    });
    }


    private Consumer<Object> firstPhase = (b) ->{
        TransactionMessage tm = (TransactionMessage) b;
        int tid = tm.getTransactionId();
            if (tm.isAborted()) {
                transactions.put(tm.getTransactionId(), 2);
                System.out.println("manager:firstphasereg -> aborting tid == " + tm.getTransactionId() + " from ");
            }
    };

    private void recover(){
        HashMap<Integer, TransactionMessage> auxiliar = new HashMap<>();
        log.recover((msg) ->{
            TransactionMessage tm = (TransactionMessage) msg;
            auxiliar.put(tm.getTransactionId(), tm);
        });
        for(TransactionMessage tm : auxiliar.values()){
            numTransactions++;
            if(tm.isPrepared()){
                transactions.put(tm.getTransactionId(), 1);
                startFullProtocol(tm);
            }
            else if(tm.isFinished()) continue;
            else{
                startHalfProtocol(tm);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        ArrayList<Address> addresses = new ArrayList<>();
        Address manager = Address.from("localhost", 20000);
        for(int i = 0; i<2; i++){
            addresses.add(Address.from("localhost",10000 + i));
        }
        Manager managerServer = new Manager(100, manager, addresses);
        managerServer.start();
       /*
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
        BufferedWriter writer = new BufferedWriter(new FileWriter("times.txt", true));
        long startTime = System.currentTimeMillis();
        int batchSize = 80;
        for (int j = 0; j < batchSize; j++) {
            ses.schedule(() -> {
                managerServer.debbugerPrototipe().thenAccept(x -> {
                    managerServer.debugCount++;
                    if(managerServer.debugCount == batchSize) {
                        long stopTime = System.currentTimeMillis();
                        long elapsedTime = stopTime - startTime;
                        try {
                            writer.append(batchSize+ " requests -> time in exec was " + (int) elapsedTime + "\n");
                            System.out.println("Finished!");
                           writer.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }, j*200, TimeUnit.MILLISECONDS);
        }*/
    }
}
