package Middleware.Recovery;

import Middleware.CausalOrder.VectorMessage;
import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.DistributedObjects.TransactionalMap;
import Middleware.TwoPhaseCommit.TransactionMessage;
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

    public void start(Consumer<Object> callback, TransactionalMap tmap, Consumer<Integer> serverStart){
        System.out.println("recovery:start -> Starting recovery");
        log.recover( (msg)->{
            if(msg instanceof VectorMessage)
                sms.causalOrderRecover(msg, callback);
            else if (msg instanceof TransactionMessage)
                tmap.transactionalRecover(msg);
            else
                callback.accept(msg);
        });
        listenToRecoveries();
        List<Integer> vector = sms.getVector();
        System.out.println("recovery:start -> Handlers registered");
        //envia o seu vetor, pq no servidor pode ter que ainda não recebeu a msg 10, mas ele pode ter recebido
        //e não ter enviado mais aseguir o que leva a que não haja ack.
        sms.sendAndReceiveToClusterRecovery("startCausalOrderRecovery", 0, 6, (a,b) -> {
            MessageRecovery mr = (MessageRecovery) b;
            //isto tudo pq um servidor pode ir abaixo a meio
            int savepoint = vector.get(mr.getId());
            int total = mr.getTotal();
            if (total != savepoint) {
                int lastId = savepoint + mr.getTotal();
                System.out.println("Savepoint= " + savepoint);
                System.out.println("There are missing messages lastId= " + lastId);
                recoveries.put(a, new RecoveredCausalOrderedMessages(total-savepoint));
                for(int i = savepoint + 1; i <= lastId; i++) {
                    sms.sendAndReceiveLoop(a, "causalOrderRecovery", i, 6)
                        .thenAccept(msg -> {
                            boolean state = recoveries.get(a).add(sms.decode(msg));
                            if (state) {
                                System.out.println("Got all recovery messages");
                                for (VectorMessage vm : recoveries.get(a).getRecoveredMessages())
                                    sms.resendMessagesRecover(vm, callback);
                                serverStart.accept(0);
                            }
                        });
                }
            }
            else{
                serverStart.accept(0);
            }
        });
        System.out.println("recovery:start -> causalOrderRecovery message sent");
    }

    private void printArray(List<Integer> v, String header){
        StringBuilder strb = new StringBuilder();
        for(Integer i : v){
            strb.append(i).append('/');
        }
        System.out.println(header + strb);
    }

    private void listenToRecoveries(){
        sms.registerOperation("startCausalOrderRecovery", (a,b) -> {
            return sms.getMissingOperationMessage();
        });

        sms.registerOperation("causalOrderRecovery", (a,b)->{
            int messageId = sms.decode(b);
            System.out.println("Request for recovery msg : id =" + messageId);
            return sms.getVectorMessage(messageId);
        });
    }

}
