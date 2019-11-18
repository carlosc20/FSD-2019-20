import Messages.Message;
import Messages.VectorMessage;
import io.atomix.utils.net.Address;

public class ClientTest {

    public static void main(String[] args) {

        Client c = new Client(Address.from(10005),"Cluster",Address.from(10000));
        c.start();

        VectorMessage m = new VectorMessage(1, "boas");
        c.send(m);

    }


}
