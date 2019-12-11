package Middleware;

import Middleware.CausalOrder.VectorMessage;
import Middleware.TwoPhaseCommit.TransactionMessage;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.List;

public class GlobalSerializer {
    private Serializer s;

    public GlobalSerializer(){
        s = new SerializerBuilder()
                .addType(VectorMessage.class)
                .addType(List.class)
                .addType(TransactionMessage.class)
                .addType(Object.class)
                .addType(Address.class)
                .build();
    }

    public Serializer getS() {
        return s;
    }
}
