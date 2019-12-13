
import java.util.concurrent.CompletableFuture;

public interface Publisher {

    CompletableFuture<Boolean> login(String username, String password);
    CompletableFuture<Boolean> register(String username, String password);

}
