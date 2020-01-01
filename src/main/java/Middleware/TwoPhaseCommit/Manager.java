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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class Manager {
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

    //TODO Não pode receber até dar recovery, clientes tem de enviar repetidamente


    public void start() {
        sms.registerCompletableOperation("startTransaction", (a, b) -> {
            TransactionMessage tm = sms.decode(b);
            //atribui id à transação e coloca-a na estrutura
            beginTransaction(tm);
            System.out.println(tm.toString());
            log.write(tm);
            //fase é para participantes e não para o manager
            tm.setPhase(1);
            return startFullProtocol(tm);
        });
    }

    private void beginTransaction(TransactionMessage tm){
        numTransactions++;
        System.out.println("manager -> transaction request id " + numTransactions);
        //Identifier ident = new Identifier(numTransactions, id);
        transactions.put(numTransactions, 1);
        tm.setTransactionId(numTransactions);
        tm.setPrepared();
    }


    public CompletableFuture<byte[]> debbuger(){
        MapMessage<String, String> mm = new MapMessage<>("marco", "123");
        TransactionMessage tm = new TransactionMessage(0,mm);
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
                                    reply.complete(s.encode(true));
                                else
                                    reply.complete(s.encode(false));
                                //System.out.println("manager:secondphase -> removing entry to tid == " + tid);
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
                    });
    }


    private Consumer<Object> firstPhase = (b) ->{
        TransactionMessage tm = (TransactionMessage) b;
        int tid = tm.getTransactionId();
            if (tm.isAborted()) {
                transactions.put(tm.getTransactionId(), 2);
                System.out.println("manager:firstphasereg -> aborting tid == " + tm.getTransactionId() + " from " + tm.getSenderId());
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
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
        BufferedWriter writer = new BufferedWriter(new FileWriter("times.txt", true));
        for (int i = 0; i < 7; i++) {
            long startTime = System.currentTimeMillis();
            for (int j = 0; j < 10; j++) {
                ses.schedule(() -> {
                    managerServer.debbuger().thenAccept(x -> {
                        long stopTime = System.currentTimeMillis();
                        long elapsedTime = stopTime - startTime;
                        try {
                            writer.append("time in exec was " + (int) elapsedTime + "\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                }, 500, TimeUnit.MILLISECONDS);
            }
        }
    }
}
