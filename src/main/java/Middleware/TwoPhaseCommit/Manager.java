package Middleware.TwoPhaseCommit;

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

public class Manager {
    private int numTransactions;
    private Map<Integer, Integer> transactions; //1-> commit, 2 -> abort
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
    }

    public void startTwoPhaseCommit(){
        sms.<TransactionMessage>registerOperation("begin", tm -> {
            //TODO tirar o tm
            beginTransaction(tm);
            return tm;
        });
        sms.<TransactionMessage>registerOperation("commit", tm ->{
            tm.setType('p');
            sms.sendAndReceiveToCluster("firstphase", tm, received->{
                if(received.getType() == 'a'){
                    System.out.println("manager -> Transaction id==" + received.getTransactionId() + "an abort has been received");
                    transactions.put(received.getTransactionId(), 2);
                }
            }).thenAccept(x ->{
                System.out.println("manager -> Transaction id==" + tm.getTransactionId() + "first phase finished");
                if(transactions.get(tm.getTransactionId()) == 1){
                    tm.setType('c');
                    System.out.println("manager-> Transaction id==" + tm.getTransactionId() + "status == commited");
                }
                else{
                    tm.setType('a');
                    System.out.println("manager -> Transaction id==" + tm.getTransactionId() + "status == aborted");
                }
                sms.sendAsyncToCluster("secondphase", tm);
            });
        });
    }

    private void beginTransaction(TransactionMessage tm){
        numTransactions++;
        System.out.println("manager -> transaction request id " + numTransactions);
        //Identifier ident = new Identifier(numTransactions, id);
        transactions.put(numTransactions, 1);
        tm.setTransactionId(numTransactions);
    }
    //TODO arranjar maneira de a não confirmação de uma transação não bloquear outras transações

    public static void main(String[] args) {
        ArrayList<Address> addresses = new ArrayList<>();
        Address manager = Address.from("localhost", 20000);
        for(int i = 0; i<3; i++){
            addresses.add(Address.from("localhost",10000 + i));
        }
        new Manager(100, manager, addresses).startTwoPhaseCommit();
    }
}
