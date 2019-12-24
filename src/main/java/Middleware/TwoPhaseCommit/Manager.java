package Middleware.TwoPhaseCommit;

import Middleware.Logging.Logger;
import Middleware.CausalOrder.CausalOrderHandler;
import Middleware.ServerMessagingService;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class Manager {
    private int id;
    private int numTransactions;
    private Map<Integer, TransactionState> transactions;
    private List<Address> staticParticipants;
    private Logger log;
    private ServerMessagingService sms;

    public Manager(int id, List<Address> participants, ServerMessagingService sms, Serializer s) {
        this.id = id;
        this.numTransactions= 0;
        this.transactions = new HashMap<>();
        this.sms = sms;
        this.staticParticipants = participants;
        //startTwoPhaseCommit();
    }
/*
    //TODO? em vez de ter char's a identificar podemos usar o typo dos send e handlers
    private void startTwoPhaseCommit(){
        sms.<TransactionMessage>registerOperation("begin", tm -> {
            beginTransaction(tm);
            return tm;
        });
        sms.registerOperation("2pc", controllerParse);
        sms.<TransactionMessage>registerOperation("commit", (tm) ->{
            tm.setType('p');
            return sms.sendAndReceiveToCluster("firstphase", tm);
        });
    }

    private void beginTransaction(TransactionMessage tm){
        numTransactions++;
        //Identifier ident = new Identifier(numTransactions, id);
        transactions.put(numTransactions, new TransactionState(staticParticipants));
        tm.setTransactionId(numTransactions);
    }

    private BiConsumer<Address, byte[]> controllerParse = (a,b) -> {
        TransactionMessage tm = sms.decode(b);
        switch (tm.getType()){
            case 'p':
                System.out.println(id + ": Received prepared");
                TransactionState ts = transactions.get(tm.getTransactionId());
                //TODO logg...se der reeboot vai ter de pedir todos os prepareds de novo
                if(ts.insertAndReadyToCommit(a)) {
                    tm.setType('c');
                    System.out.println(id + ": Sending commit to cluster");
                    sms.sendAsyncToCluster("2pc", tm);
                    }
                    //TODO cuidado que aqui entram dois casos, mas não deve de acontecer == 1
                else{
                    tm.setType('a');
                    sms.sendAsyncToCluster("2pc", tm);
                    }
                }
                break;
            case 'a':
                //TODO logg

                break;
        };
    */
    //TODO arranjar maneira de a não confirmação de uma transação não bloquear outras transações
}
