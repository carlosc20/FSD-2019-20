
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class PublisherImpl implements Publisher {

    private HashMap<String, User> users;

    public PublisherImpl() {
        this.users = new HashMap<>();
    }

    @Override
    public CompletableFuture<Boolean> login(String username, String password) {
        User user = users.get(username);
        return CompletableFuture.completedFuture(user.getPassword().equals(password));
    }

    @Override
    public CompletableFuture<Boolean> register(String username, String password) {
        if(users.containsKey(username))
            return CompletableFuture.completedFuture(false);

        User newUser = new User(username,password);
        users.put(username, newUser);

        return CompletableFuture.completedFuture(true);
    }

}
