package Middleware.TwoPhaseCommit;

import Middleware.Logging.Logger;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Controller {
    private ManagedMessagingService mms;
    //(Id da transação, (Respostas dos participantes))
    private Map<Integer, TransactionState> transactions;
    private List<Address> staticParticipants;
    private Serializer s;
    private Logger l;

    public Controller(ManagedMessagingService mms, List<Address> participants, Serializer s){
        this.mms = mms;
        this.transactions = new HashMap<>();
        this.staticParticipants = participants;
        this.s = s;
    }

    //pseudo-código (à espera de resposta do stor.....)
    /*
    public void startProtocol(){
        mms.registerHandler("two-phase-protocol", (a,b) ->{

             m = s.decode(b);
            TransactionState ts = transactions.get(b.getTransactionId)
            if(m.type == "p")
                if(ts.readyToCommit(a))
                    sendCommitMsg()
            //abort
            else{
                //ainda não sei o que podemos fazer aqui...esperar até estar bom suponho
            }

        });
    }

    public void sendCommitMsg();
*/
}
