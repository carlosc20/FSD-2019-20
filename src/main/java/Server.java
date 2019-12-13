import Middleware.MessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.Map;

public class Server {

    private MessagingService ms; // atomix para clientes e coh para servidores

    private Serializer s = new SerializerBuilder()
            .addType(ArrayList.class)
            .addType(String.class)
            .build();

    private Publisher publisher = new PublisherImpl();
    private Map<Address,Session> sessions;
    private Feed feed;

    // meter send to cluster onde é preciso e criar handlers para sincronizar

    // Exemplos de pedidos ao Publisher

    // recebe login
    public void login() {

        // recebes do handler
        Address address = null;
        byte[] recebido = null;

        MessageAuth msg = s.decode(recebido); // vês o objeto que o stub manda
        Session session = new SessionImpl(feed, publisher.getUser()); // ver melhor
        sessions.put(address,session);
        ms.send(s.encode(publisher.login(msg.getUsername(),msg.getPassword())),"login");

    }

    // recebe register
    public void register() {

        // recebes do handler
        byte[] recebido = null;

        MessageAuth msg = s.decode(recebido);
        ms.send(s.encode(publisher.register(msg.getUsername(),msg.getPassword())),"register");
    }


    // Exemplo de pedido á session:

    // recebe getSubs
    public void getSubs() {

        // recebes do handler
        Address address = null;

        Session session = sessions.get(address);
        ms.send(s.encode(session.getSubscriptions()),"getSubs");
    }

}
