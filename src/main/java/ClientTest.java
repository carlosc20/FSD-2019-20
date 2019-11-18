
import io.atomix.utils.net.Address;

public class ClientTest {

    public static void main(String[] args) {

        Client c = new Client(Address.from(10010),"Cluster",Address.from(10000));
        c.start();
        c.send("Olá");

    }


}
