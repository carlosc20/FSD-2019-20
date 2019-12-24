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
    private Map<Integer, Integer> transactions; //1-> commit, 2 -> abort
    private List<Address> staticParticipants;
    private Logger log;
    private ServerMessagingService sms;

    public Manager(int id, List<Address> participants, ServerMessagingService sms, Serializer s) {
        this.id = id;
        this.numTransactions= 0;
        this.transactions = new HashMap<>();
        this.sms = sms;
        this.staticParticipants = participants;

    }

    public void startTwoPhaseCommit(){
        sms.<TransactionMessage>registerOperation("begin", tm -> {
            beginTransaction(tm);
            return tm;
        });
        sms.<TransactionMessage>registerOperation("commit", tm ->{
            tm.setType('p');
            sms.sendAndReceiveToCluster("firstphase", tm, received->{
                if(received.getType() == 'a')
                    transactions.put(received.getTransactionId(), 2);
                return received;
            }).thenAccept(x ->{
                if(transactions.get(tm.getTransactionId()) == 1)
                    tm.setType('c');
                else
                    tm.setType('a');
                sms.sendAsyncToCluster("secondphase", tm);
            });
        });
    }

    private void beginTransaction(TransactionMessage tm){
        numTransactions++;
        //Identifier ident = new Identifier(numTransactions, id);
        transactions.put(numTransactions, 1);
        tm.setTransactionId(numTransactions);
    }

    //TODO arranjar maneira de a não confirmação de uma transação não bloquear outras transações
}
