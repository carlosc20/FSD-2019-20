import io.atomix.utils.net.Address;

public class ServerTest {
    public static void main(String[] args) {

        new Server(Address.from(25000),"Cluster",null).start();
    }
}
