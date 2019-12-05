package Middleware.TwoPhaseCommit;

import Middleware.CausalOrdering.CausalOrderHandler;
import Middleware.CausalOrdering.VectorMessage;
import Middleware.Logging.Logger;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {
    private int transactionId;
    private ManagedMessagingService mms;
    private Map<Integer, TransactionState> transactions;
    private List<Address> staticParticipants;
    private Serializer s;
    private Logger l;
    private CausalOrderHandler coh;
    private ExecutorService e;

    public Manager(ManagedMessagingService mms, List<Address> participants, CausalOrderHandler coh){
        this.transactionId = 0;
        this.mms = mms;
        this.transactions = new HashMap<>();
        this.staticParticipants = participants;
        //TODO ver este serializer
        this.s = new SerializerBuilder()
                .addType(VectorMessage.class)
                .addType(List.class)
                .build();
        this.coh = coh;
        e = Executors.newFixedThreadPool(1); // para já...não sei
    }

    public void startProtocol(){
        mms.start();
        mms.registerHandler("controller", (a,b) ->{
            TransactionMessage m = s.decode(b);
            switch (m.getResponse()){
                case 'p':
                    TransactionState ts = transactions.get(m.getTransactionId());
                    if(ts.insertAndReadyToCommit(a)) {
                        m.setResponse('c');
                        //TODO ver melhor este sendToCluster
                        //coh.sendToCluster(m, s, "participant");
                    }
                    break;
                case 'r':
                    transactionId++;
                    transactions.put(transactionId, new TransactionState(staticParticipants));
                    m.setTransactionId(transactionId);
                    m.setResponse('p');
                    //coh.sendToCluster(m, s, "participant");
                    break;
                case 'a':

                    break;
                }
        },e);
    }

    CompletableFuture<Boolean> sendRequest(Address coordenator){
        TransactionMessage m = new TransactionMessage();
        return mms.sendAndReceive(coordenator, "controller", s.encode(m))
                      .thenApply((response)->{
                            TransactionMessage r = s.decode(response);
                            //Fazer com que o controlador não mande o prepared para quem pediu?
                            if(r.getResponse()=='c') return true;
                            else return false;
                        }
                      );
    }
    //Todo função que verifica se pode ser enviada ou que envia mesmo?
    //TODO arranjar maneira de a não confirmação de uma transação não bloquear outras transações
    //public ... recover(file...)
}
