package Middleware;

import Logic.Post;
import Logic.User;
import Middleware.CausalOrder.VectorMessage;
import Middleware.Logging.SubscriptionLog;
import Middleware.Logging.UnsubscriptionLog;
import Middleware.Marshalling.*;
import Middleware.Recovery.MessageRecovery;
import Middleware.TwoPhaseCommit.DistributedObjects.MapMessage;
import Middleware.TwoPhaseCommit.Identifier;
import Middleware.TwoPhaseCommit.TransactionMessage;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.List;


public class GlobalSerializer {
    private SerializerBuilder sb;

    public GlobalSerializer(){
        sb = new SerializerBuilder()
                .addType(VectorMessage.class)
                .addType(List.class)
                .addType(ArrayList.class)
                .addType(Identifier.class)
                .addType(TransactionMessage.class)
                .addType(Address.class)
                .addType(MessageAuth.class)
                .addType(MessageSend.class)
                .addType(MessageSub.class)
                .addType(MessageReply.class)
                .addType(MessageRecovery.class)
                .addType(MapMessage.class)
                .addType(SubscriptionLog.class)
                .addType(UnsubscriptionLog.class)
                .addType(Post.class)
                .addType(User.class);
    }

    public Serializer build(){
        return sb.build();
    }
}
