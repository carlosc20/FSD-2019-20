package Middleware.TwoPhaseCommit;

import Middleware.Logging.Logger;
import Middleware.CausalOrder.CausalOrderHandler;
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

//TODO relógio adicional no cliente para saber o que já leu
//TODO PERGUNTAR AO CÉSAR COMO É QUE EXECUTA TRANSAÇÕES RECUPERADAS
public class Manager {
    private int id;
    private int numTransactions;
    private Map<Identifier, TransactionState> transactions;
    private List<Address> staticParticipants;
    private Logger l;
    private ManagedMessagingService mms;
    private CausalOrderHandler<TransactionMessage> coh;
    private ExecutorService e;
    private Serializer s;

    public Manager(int id, List<Address> participants, ManagedMessagingService mms, CausalOrderHandler coh, Serializer s) {
        this.id = id;
        this.numTransactions= 0;
        this.transactions = new HashMap<>();
        this.mms = mms;
        this.coh = coh;
        //lista não inclui o próprio servidor
        this.staticParticipants = participants; // para enviar msg 2pc para ele próprio
        e = Executors.newFixedThreadPool(1);
    }

    //TODO? em vez de ter char's a identificar podemos usar o typo dos send e handlers
    public void startTwoPhaseCommit(){
        mms.registerHandler("2pc", controllerParse, e);
    }

    public CompletableFuture<Void> sendOrderedOperationToCluster(Object content, String type){
        TransactionMessage tm = buildTransaction(content);
        return coh.sendAsyncToCluster(staticParticipants, type, tm)
                    .thenAccept(x -> {
                        //TODO log
                        System.out.println(id + ": Sending prepared request");
                        tm.setContent(null);
                        sendAsyncToCluster(tm, "2pc");});
    }

    public CompletableFuture<Void> sendOperationToCluster(Object content, String type){
        TransactionMessage tm = buildTransaction(content);
        return sendAsyncToCluster(tm, type)
                .thenAccept(x -> {
                    //TODO log
                    System.out.println(id + ": Sending prepared request");
                    tm.setContent(null);
                    sendAsyncToCluster(tm, "2pc");});
    }

    private TransactionMessage buildTransaction(Object content){
        System.out.println(id + ": Sending object to temporary status");
        numTransactions++;
        Identifier ident = new Identifier(numTransactions, id);
        transactions.put(ident, new TransactionState(staticParticipants));
        TransactionMessage tm = new TransactionMessage(ident, 't', content);
        return tm;
    }

    //TODO daria para por tudo genérico : String = 2pc + type nos dois lados, mas tem o problema de recuperação
    // resolução guardar os consumers para cada tipo
    private BiConsumer<Address, byte[]> controllerParse = (a,b) -> {
        TransactionMessage tm = s.decode(b);
        switch (tm.getType()){
            case 'p':
                System.out.println(id + ": Received prepared");
                TransactionState ts = transactions.get(tm.getTransactionId());
                //TODO logg...se der reeboot vai ter de pedir todos os prepareds de novo
                if(ts.insertAndReadyToCommit(a)) {
                    tm.setType('c');
                    System.out.println(id + ": Sending commit to cluster");
                    sendAsyncToCluster(tm, "2pc");
                }
                break;
            case 'a':
                //TODO logg
                //para já nada
                break;
        }
    };

    public CompletableFuture<Void> sendAsyncToCluster(TransactionMessage tm, String type){
        for (Address a : staticParticipants)
            mms.sendAsync(a, type, s.encode(tm));
        return CompletableFuture.completedFuture(null);
    }

    //TODO arranjar maneira de a não confirmação de uma transação não bloquear outras transações
    //public ... recover(file...)
}
