package Middleware.TwoPhaseCommit;

import Middleware.CausalOrdering.CausalOrderHandler;
import Middleware.Logging.Logger;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {
   /*
    private int serverId;
    private int transactionId;
    private ManagedMessagingService mms;
    private Map<Identifier, TransactionState> transactions;
    private List<Address> staticParticipants;
    private Serializer s;
    private Logger l;
    private CausalOrderHandler coh;
    private ExecutorService e;

    public Manager(int id, ManagedMessagingService mms, List<Address> participants, CausalOrderHandler coh){
        this.serverId = id;
        this.transactionId = 0;
        this.mms = mms;
        this.transactions = new HashMap<>();
        this.staticParticipants = participants;
        this.s = TransactionMessage.serializer;
        this.coh = coh;
        e = Executors.newFixedThreadPool(1); // para já...não sei
    }

    public void startProtocol(){
        mms.registerHandler("controller", (a,b) ->{
            TransactionMessage m = s.decode(b);
            switch (m.getResponse()){
                case 'p':
                    TransactionState ts = transactions.get(m.getIdent());
                    if(ts.insertAndReadyToCommit(a)) {
                        m.setResponse('c');
                        //TODO ver melhor este sendToCluster
                        coh.sendToCluster(m, s, "participant");
                    }
                    break;
                case 'r':
                    transactionId++;
                    Identifier ident = new Identifier(transactionId,serverId);
                    transactions.put(ident, new TransactionState(staticParticipants));
                    m.setIdent(ident);
                    m.setResponse('p');
                    coh.sendToCluster(m, s, "participant");
                    break;
                case 'a':
                    //TODO qualquer coisa
                    break;
                }
        },e);
        mms.registerHandler("participant", (a,b) ->{
            TransactionMessage m = s.decode(b);
            switch (m.getResponse()) {
                case 'p':
                    m.setResponse('p');
                    //TODO TESTAR!!! o cenas do address
                    coh.sendAnswer(m, s, "controller", Address.from(a.address().getHostAddress()));
                    break;
                case 'c':
                    //logg??
                    //envia para todos??
                    break;
                case 'a':
                    //não sei
                    break;
            }
        },e);
    }
    /*
    /*
    //isto seria no servidor
    registerHandler("topics", (a,b) ->{
        manager.sendRequest().thenCompose(sendTopic)
        })
     */
    /*
    CompletableFuture<Boolean> sendRequest(Address coordenator, Content ){
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
    */
    //Todo função que verifica se pode ser enviada ou que envia mesmo?
    //TODO arranjar maneira de a não confirmação de uma transação não bloquear outras transações
    //public ... recover(file...)
}
