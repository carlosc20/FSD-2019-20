package Middleware.TwoPhaseCommit;

import Middleware.DistributedStructures.MapMessage;
import Middleware.GlobalSerializer;
import Middleware.Logging.Logger;
import Middleware.CausalOrder.CausalOrderHandler;
import Middleware.ServerMessagingService;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class Manager {
    private int numTransactions;
    private Map<Integer, TransactionState> transactions;
    private List<Address> staticParticipants;
    private Serializer s;
    private Logger log;
    private ServerMessagingService sms;

    public Manager(int id, Address address, List<Address> participants) {
        this.numTransactions= 0;
        this.transactions = new HashMap<>();
        this.s = new GlobalSerializer().getS();
        this.log = new Logger("logs", "Manager", s);
        this.sms = new ServerMessagingService(id, address, participants, log);
        this.staticParticipants = participants;
        start();
    }

    public void start(){
        sms.registerOperation("startTransaction", (a,b) -> {
            TransactionMessage tm = sms.decode(b);
            beginTransaction(tm);
            log.write(tm);
            sms.sendAsyncToCluster("firstphase", tm);
            return sms.encode(0);
        });

        sms.registerOperation("firstphase", (a,b) -> {
            TransactionMessage tm = sms.decode(b);
            TransactionState ts = transactions.get(tm.getTransactionId());
            //1º if -> caso de uma confirmação repedida
            if(ts.getFirstPhaseNotFinishedCounter() != 0){
                System.out.println("manager:firstphasereg -> received a reponse " + tm.toString());
                if(tm.isAborted()) ts.setAborted();
                if(ts.insertAndAllAnsweredFirstPhase(a)){
                    if(ts.isAborted()){
                        tm.setAborted();
                        System.out.println("manager:firstphasereg -> aborting tid == " + tm.getTransactionId());
                    }
                    else{
                        tm.setCommited();
                        System.out.println("manager:firstphasereg -> commiting tid == " + tm.getTransactionId());
                    }
                    log.write(tm);
                    sms.sendAsyncToCluster("secondphase", tm);
                }
            }
        });

        sms.registerOperation("secondphase", (a,b) ->{
            TransactionMessage tm = sms.decode(b);
            int tid = tm.getTransactionId();
            //1º if -> caso de uma confirmação repedida
            if(transactions.containsKey(tid)){
                System.out.println("manager:secondphasereg -> Received second-phase confirmation from " + a);
                TransactionState ts = transactions.get(tid);
                if(ts.insertAndAllAnsweredSecondPhase(a)){
                    System.out.println("manager:secondphasereg -> removing entry to tid == " + tm.getTransactionId());
                    transactions.remove(tid);
                }
            }
        });

        sms.registerOperation("recovery", (a,b) -> {
            int requested = sms.decode(b);
            int maxSize = transactions.size();
            for(int i = requested; i<maxSize; i++){
                TransactionMessage tm = new TransactionMessage(i, transactions.get(i).getContent());
                //acho que enviar para um resolve
                sms.sendAsync(a,"firstphase", tm);
            }
        });
    }

    private void beginTransaction(TransactionMessage tm){
        numTransactions++;
        System.out.println("manager -> transaction request id " + numTransactions);
        //Identifier ident = new Identifier(numTransactions, id);
        transactions.put(numTransactions, new TransactionState(staticParticipants, tm.getContent()));
        tm.setTransactionId(numTransactions);
        tm.setPrepared();
    }

    private void recover(){
        HashMap<Integer, TransactionMessage> auxiliar = new HashMap<>();
        log.recover((msg) ->{
            TransactionMessage tm = (TransactionMessage) msg;
            auxiliar.put(tm.getTransactionId(), tm);
        });
        for(TransactionMessage tm : auxiliar.values()){
            if(tm.isPrepared()){
                transactions.put(tm.getTransactionId(), new TransactionState(staticParticipants, tm.getContent()));
                sms.sendAsyncToCluster("firstphase", tm);
            }
            else{
                TransactionState ts = new TransactionState(staticParticipants, tm.getContent());
                ts.firstPhaseFinished();
                transactions.put(tm.getTransactionId(), ts);
                sms.sendAsyncToCluster("secondphase", tm);
            }
        }
    }

    public static void main(String[] args) {
        ArrayList<Address> addresses = new ArrayList<>();
        Address manager = Address.from("localhost", 20000);
        for(int i = 0; i<2; i++){
            addresses.add(Address.from("localhost",10000 + i));
        }
        new Manager(100, manager, addresses).start();
    }
}
