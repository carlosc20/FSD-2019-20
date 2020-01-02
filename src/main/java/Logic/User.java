package Logic;

import java.util.HashSet;
import java.util.Set;

public class User {

    private String name;
    private String password;
    private Set<String> subscriptions;

    public User(String name, String password) {
        this.name = name;
        this.password = password;
        subscriptions = new HashSet<>();
    }

    public String getName() {
        return name;
    }

    public String getPassword() {
        return password;
    }

    public Set<String> getSubscriptions() {
        return subscriptions;
    }

    public void addSubscription(String name) {
        subscriptions.add(name);
    }

    public void removeSubscription(String name) {
        subscriptions.remove(name);
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
