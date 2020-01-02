package Middleware.Logging;

import Middleware.Marshalling.MessageSub;

public class UnsubscriptionLog {
    private MessageSub ms;

    public UnsubscriptionLog(MessageSub ms){
        this.ms = ms;
    }

    public MessageSub getMs() {
        return ms;
    }
}
