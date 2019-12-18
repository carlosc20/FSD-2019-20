package Middleware.Marshalling;


public class MessageSub extends MessageAuth {

    private String name;

    public MessageSub(String username, String password, String name) {
        super(username, password);
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
