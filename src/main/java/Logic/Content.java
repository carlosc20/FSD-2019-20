package Logic;

import Middleware.TwoPhaseCommit.TransactionMessage;

public class Content extends TransactionMessage {
    private String msg;

    public Content(String msg){
        super();
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }
}
