package Logic;

import Middleware.DistributedStructures.Mapped;

import java.util.ArrayList;
import java.util.List;

public class User implements Mapped<String> {

    private String name;
    private String password;
    private List<String> subscriptions;

    public User(String name, String password) {
        this.name = name;
        this.password = password;
        subscriptions = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public String getPassword() {
        return password;
    }


    public List<String> getSubscriptions() {
        return subscriptions;
    }

    public void addSubscription(String name) {
        subscriptions.add(name);
    }

    public void removeSubscription(String name) {
        subscriptions.remove(name);
    }

    public String getKey(){
        return name;
    }
}
