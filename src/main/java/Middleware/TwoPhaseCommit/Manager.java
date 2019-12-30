package Middleware.TwoPhaseCommit;

import Middleware.GlobalSerializer;
import Middleware.Logging.Logger;
import Middleware.Marshalling.MessageAuth;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
        this.s = new GlobalSerializer().build();
        this.log = new Logger("logs", "Manager", s);
        this.sms = new ServerMessagingService(id, address, participants, log, s);
        this.staticParticipants = participants;
        sms.start();
        recover();
        //TODO sendAndReceive no startTransaction, até já tem o inicio
        start();
    }

    //TODO Não pode receber até dar recovery, clientes tem de enviar repetidamente

    public void start() {
        sms.registerOperation("startTransaction", (a, b) -> {
            TransactionMessage tm = sms.decode(b);
            beginTransaction(tm);
            sms.sendAndReceiveLoopToCluster("firstphase", tm, Duration.ofSeconds(10), firstPhase);
            return sms.encode(0);
        });

        sms.registerOperation("transactionalRecovery", (a,b) -> {
            int requested = sms.decode(b);
            int maxSize = transactions.size();
            System.out.println(requested+  " and maxSize = " + maxSize);
            if(requested >= maxSize) return sms.encode(false);
            for(int i = requested; i<maxSize; i++){
                TransactionMessage tm = new TransactionMessage(i, transactions.get(i).getContent());
                //acho que enviar para um resolve
                sms.sendAsync(a,"firstphase", tm);
            }
            return sms.encode(true);
        });
    }

    private Consumer<Object> firstPhase = (b) ->{
        TransactionMessage tm = (TransactionMessage) b;
        int tid = tm.getTransactionId();
        TransactionState ts = transactions.get(tid);
        //1º if -> caso de uma confirmação repedida
        //TODO manager iniciar depois dos participantes
        System.out.println("manager:firstphasereg -> received a reponse " + tm.toString());
        if(tm.isAborted()) ts.setAborted();
        if(ts.insertAndAllAnsweredFirstPhase(tm.getSenderId())) {
            if (ts.isAborted()) {
                tm.setAborted();
                System.out.println("manager:firstphasereg -> aborting tid == " + tm.getTransactionId());
            } else {
                tm.setCommited();
                System.out.println("manager:firstphasereg -> commiting tid == " + tm.getTransactionId());
            }
            log.write(tm);
            sms.sendAndReceiveLoopToCluster("secondphase", tm, Duration.ofSeconds(6), (b2) -> secondPhase(b2));
        }
    };

    private void secondPhase(Object b){
        TransactionMessage tm = (TransactionMessage) b;
        int tid = tm.getTransactionId();
        //1º if -> caso de uma confirmação repedida
        if (transactions.containsKey(tid)) {
            System.out.println("manager:secondphasereg -> Received second-phase confirmation from " + tm.getSenderId());
            TransactionState ts2 = transactions.get(tid);
            if (ts2.insertAndAllAnsweredSecondPhase(tm.getSenderId())) {
                System.out.println("manager:secondphasereg -> removing entry to tid == " + tm.getTransactionId());
                tm.setFinished();
                log.write(tm);
                transactions.remove(tid);
            }
        }
    }

    private void beginTransaction(TransactionMessage tm){
        numTransactions++;
        System.out.println("manager -> transaction request id " + numTransactions);
        //Identifier ident = new Identifier(numTransactions, id);
        transactions.put(numTransactions, new TransactionState(staticParticipants, tm.getContent()));
        tm.setTransactionId(numTransactions);
        tm.setPrepared();
        log.write(tm);
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
            else if(tm.isFinished()) continue;
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
