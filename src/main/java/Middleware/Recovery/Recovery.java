package Middleware.Recovery;

import Middleware.CausalOrder.VectorMessage;
import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.DistributedObjects.TransactionalMap;
import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

public class Recovery {
    private ServerMessagingService sms;
    private Logger log;
    private HashMap<Address, RecoveredCausalOrderedMessages> recoveries;

    public Recovery(Logger log, ServerMessagingService sms){
        this.log = log;
        this.sms = sms;
        this.recoveries = new HashMap<>();
    }

    public void start(Consumer<Object> callback, TransactionalMap tmap){
        System.out.println("recovery:start -> Starting recovery");
        listenToRecoveries();
        log.recover( (msg)->{
            if(msg instanceof VectorMessage)
                sms.causalOrderRecover(msg, callback);
            else
                tmap.transactionalRecover(msg);
        });

        List<Integer> vector = sms.getVector();
        System.out.println("recovery:start -> Handlers registered");
        //envia o seu vetor, pq no servidor pode ter que ainda não receu a msg 10, mas ele pode ter recebido
        //e não ter enviado mais aseguir o que leva a que não haja ack.
        sms.sendAndReceiveToClusterRecovery("startCausalOrderRecovery", vector, 6, (a,b) -> {
            MessageRecovery mr = (MessageRecovery) b;
            //isto tudo pq um servidor pode ir abaixo a meio
            int savepoint = vector.get(mr.getId());
            int total = mr.getTotal();
            if (total != savepoint) {
                int lastId = savepoint + mr.getTotal();
                recoveries.put(a, new RecoveredCausalOrderedMessages(total));
                for (int i = savepoint; i < lastId; i++) {
                    sms.sendAndReceiveLoop(a, "causalOrderRecovery", sms.encode(savepoint), 6)
                        .thenAccept(msg -> {
                            boolean state = recoveries.get(a).add(sms.decode(msg));
                            if (state) {
                                for (VectorMessage vm : recoveries.get(a).getRecoveredMessages())
                                    sms.causalOrderRecover(vm, callback);
                            }
                        });
                }
            }
        });
        System.out.println("recovery:start -> causalOrderRecovery message sent");
    }


    private void listenToRecoveries(){
        sms.registerOperation("startCausalOrderRecovery", (a,b) -> {
            List<Integer> vector = sms.decode(b);
            return sms.getMissingOperationMessage(vector);
        });

        sms.registerOperation("causalOrderRecovery", (a,b)->{
            int messageId = sms.decode(b);
            return sms.getVectorMessage(messageId);
        });
    }

}
