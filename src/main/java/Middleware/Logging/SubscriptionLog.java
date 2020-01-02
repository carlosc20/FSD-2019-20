package Middleware.Logging;

import Middleware.Marshalling.MessageSub;

public class SubscriptionLog {
    private MessageSub ms;

    public SubscriptionLog(MessageSub ms){
        this.ms = ms;
    }

    public MessageSub getMs() {
        return ms;
    }
}
