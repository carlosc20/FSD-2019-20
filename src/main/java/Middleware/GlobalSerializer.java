package Middleware;

import Logic.User;
import Middleware.CausalOrder.VectorMessage;
import Middleware.TwoPhaseCommit.DistributedObjects.MapMessage;
import Middleware.Marshalling.MessageAuth;
import Middleware.Marshalling.MessageRecovery;
import Middleware.Marshalling.MessageSend;
import Middleware.Marshalling.MessageSub;
import Middleware.TwoPhaseCommit.TransactionMessage;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.List;


//TODO usar as cenas custom
public class GlobalSerializer {
    SerializerBuilder sb;

    public GlobalSerializer(){
        sb = new SerializerBuilder()
                .addType(VectorMessage.class)
                .addType(List.class)
                .addType(ArrayList.class)
                .addType(byte[].class)
                .addType(TransactionMessage.class)
                .addType(Address.class)
                .addType(MessageAuth.class)
                .addType(MessageSend.class)
                .addType(MessageSub.class)
                .addType(MessageRecovery.class)
                .addType(MapMessage.class)
                .addType(User.class);
    }

    public void addType(Class<?> type){
        sb.addType(type);
    }

    public Serializer build(){
        return sb.build();
    }
}
