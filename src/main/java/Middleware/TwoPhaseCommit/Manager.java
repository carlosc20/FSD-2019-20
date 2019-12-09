package Middleware.TwoPhaseCommit;

import Middleware.CausalOrdering.CausalOrderHandler;
import Middleware.Logging.Logger;
import io.atomix.utils.net.Address;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class Manager {
    private int id;
    private int numTransactions;
    private Map<Integer, TransactionState> transactions;
    private List<Address> staticParticipants;
    private Logger l;
    private CausalOrderHandler<TransactionMessage> coh;
    private ExecutorService e;

    public Manager(int id, List<Address> participants, Address myAddr) {
        this.id = id;
        this.numTransactions= 0;
        this.transactions = new HashMap<>();
        //TODO retirar o manager caso esteja na lista
        this.staticParticipants = participants;
        this.coh = new CausalOrderHandler<>(id, participants, myAddr);
        e = Executors.newFixedThreadPool(1);
    }

    public void startProtocol(){
        //TODO resolver o martelanço
        coh.registerHandlerMartelado("controller", e, parseAndExecute);
        //coh.registerHandler("participant", e , (a,b) -> {});
    }

    private BiConsumer<Object, Address> parseAndExecute = (o, a) -> {
        TransactionMessage tm = (TransactionMessage) o;
        switch (tm.getType()){
            case 'b':
                System.out.println(id + ": Received begin request");
                numTransactions++;
                transactions.put(numTransactions,new TransactionState(staticParticipants));
                tm.setTransactionId(numTransactions);
                //TODO logg
                coh.sendAsyncMartelado(tm, "forController", a);
                //coh.sendAsyncToCluster(tm, "participant");
                break;
            case 't':
                System.out.println(id + ": Received transaction request");
                tm.setType('p');
                coh.sendAsyncToCluster(tm, "participant");
                break;
            case 'p':
                System.out.println(id + ": Received prepared");
                TransactionState ts = transactions.get(tm.getTransactionId());
                //TODO logg
                if(ts.insertAndReadyToCommit(a)) {
                    tm.setType('c');
                    System.out.println(id + ": Sending commit to cluster");
                    coh.sendAsyncToCluster(tm, "participant");
                }
                break;
            case 'a':
                //TODO logg
                //para já nada
                break;
        }
    };

    //TODO arranjar maneira de a não confirmação de uma transação não bloquear outras transações
    //public ... recover(file...)
}
