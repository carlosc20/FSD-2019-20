import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TestAtomix {

    public static class Msg{
        int a;
        int b;
    }

    public static void main(String[] args) throws Exception {

        ScheduledExecutorService e = Executors.newScheduledThreadPool(1);

        Serializer s = new SerializerBuilder()
                .addType(Msg.class)
                .build();

        final ArrayList<Address> members = new ArrayList<>();
        final ArrayList<ManagedMessagingService> services = new ArrayList<>();

        for (int i = 1; i < 4; i++) {
            Address address = Address.from(10000 + i);
            members.add(address);

            ManagedMessagingService ms = new NettyMessagingService(
                    "teste",
                    address,
                    new MessagingConfig());
            ms.start();
            services.add(ms);
            // Receber
            ms.registerHandler("msg", (a,b)-> {
                Msg m = s.decode(b);
                System.out.println(address.port() +  " recebeu '"+ m.b +"' de " + m.a);
            }, e);

        }


        // Enviar
        for (ManagedMessagingService mss : services) {
            Msg m = new Msg();
            m.a =1;
            m.b=2;

            for(Address address : members){
                if(mss.address().equals(address)) continue;
                mss.sendAsync(address, "msg", s.encode(m));
            }
        }





        while(true);
    }
}