package Middleware;

import Middleware.CausalOrder.VectorMessage;
import Middleware.TwoPhaseCommit.TransactionMessage;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.List;


//TODO pode vir a ser custom
public class GlobalSerializer {
    Serializer s;

    public GlobalSerializer(){
        s = new SerializerBuilder()
                .addType(VectorMessage.class)
                .addType(List.class)
                .addType(ArrayList.class)
                .addType(TransactionMessage.class)
                .addType(Address.class)
                .addType(MessageAuth.class)
                .build();
    }

    public Serializer getS(){
        return s;
    }
}
