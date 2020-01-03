import Middleware.TwoPhaseCommit.Manager;
import io.atomix.utils.net.Address;

import java.util.ArrayList;

public class ServerTest {

    public static void main(String[] args) {
        int[] servers = Config.servers;
        ArrayList<Address> addresses = new ArrayList<>();
        for (int s : servers) addresses.add(Address.from(s));

        Address manager = Address.from(20000);

        for (int i = 0; i < servers.length; i++) {
            new Server(i, addresses.get(i), addresses, manager);
        }
        new Manager(100, manager, addresses);
    }
}
