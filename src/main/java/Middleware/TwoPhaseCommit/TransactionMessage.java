package Middleware.TwoPhaseCommit;

import Middleware.CausalOrdering.VectorMessage;

public class TransactionMessage extends VectorMessage {
    private Identifier ident;
    private char response;

    public TransactionMessage(){
        this.ident = null;
        this.response = 'r';
    }

    public TransactionMessage(int transactionId, int serverId,  char response){
        this.ident = new Identifier(transactionId, serverId);
        this.response = response;
    }

    public char getResponse() {
        return response;
    }

    public Identifier getIdent(){
        return ident;
    }

    public void setResponse(char response) {
        this.response = response;
    }

    public void setIdent(int transactionId, int serverId) {
        this.ident = new Identifier(transactionId, serverId);
    }
    public void setIdent(Identifier ident) {
        this.ident = ident;
    }
}
