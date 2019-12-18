package Middleware.Marshalling;

public class MessageAuth {

    private String username;
    private String password;

    public MessageAuth(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public String toString() {
        return "Middleware.Marshalling.MessageAuth{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
