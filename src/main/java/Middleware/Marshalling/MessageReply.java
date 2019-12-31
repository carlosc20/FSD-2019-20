package Middleware.Marshalling;

public class MessageReply<T> {
    private int responseStatusCode;
    private T content;


    public MessageReply(T content) {
        this.responseStatusCode = 0;
        this.content = content;
    }


    public MessageReply(int responseStatusCode, T content) {
        this.responseStatusCode = responseStatusCode;
        this.content = content;
    }



    public static MessageReply OK = new MessageReply<>(null);
    public static MessageReply ERROR(int code) {
        return new MessageReply<>(code, null);
    }


    public int getResponseStatusCode() {
        return responseStatusCode;
    }

    public T getContent() {
        return content;
    }
}
