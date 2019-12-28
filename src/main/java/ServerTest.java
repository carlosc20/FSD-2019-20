import io.atomix.utils.net.Address;

import java.util.ArrayList;

public class ServerTest {

    public static void main(String[] args) {

        int n = 10;

        ArrayList<Address> servers = new ArrayList<>();
        Address manager = Address.from(20000);

        for(int i = 0; i<n; i++){
            Address add = Address.from(10000 + i);
            new Server(i, add, servers, manager).start();
            servers.add(add);
        }

    }

}
