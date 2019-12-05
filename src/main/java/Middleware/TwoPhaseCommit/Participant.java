package Middleware.TwoPhaseCommit;

import Middleware.CausalOrdering.CausalOrderHandler;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Participant {
    private Address manager;
    private ManagedMessagingService mms;
    private CausalOrderHandler<TransactionMessage> coh;
    private ExecutorService e;
    private Serializer s;

    public Participant(int id, List<Address> servers, Address manager, ManagedMessagingService mms){
        this.manager = manager;
        this.mms = mms;
        mms.start();
        ArrayList<Class> param = new ArrayList<>();
        param.add(TransactionMessage.class);
        param.add(Object.class);
        this.coh = new CausalOrderHandler<>(id, mms, servers, param);
        this.e = Executors.newFixedThreadPool(1);
        this.s = coh.getMsgSerializer();
    }

    public void startListening() {
        mms.registerHandler("participant", (a, b) -> {
            coh.read(b, (msg)-> {
                TransactionMessage tm = (TransactionMessage) msg.getContent();
                parseAndExecute(tm);
            });
        }, e);
    }

    private void sendMessage(Object topic){
        //begin()
        TransactionMessage beginMessage = new TransactionMessage();
        coh.sendAndReceive(beginMessage, "controller", manager, e, (msg1)->{
            TransactionMessage transaction = (TransactionMessage) msg1.getContent();
            transaction.setContent(topic);
            //utilização de recursos
            coh.sendToCluster(transaction, "participant");
            //Commit()
            transaction.setContent(null);
            coh.sendAndReceive(transaction, "controller", manager, e, (msg2)->{
            parseAndExecute( (TransactionMessage) msg2.getContent());
            });

        });
    }
    //Muito incompleto
    private void parseAndExecute(TransactionMessage tm) {
        switch (tm.getResponse()) {
            case 'p':
                tm.setResponse('p');
                coh.send(tm, "controller", manager);
                break;
            case 'c':
                //meter log
                break;
            case 'a':
                //não sei
                break;
        }
    }
}
