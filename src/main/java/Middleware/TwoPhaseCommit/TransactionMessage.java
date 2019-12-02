package Middleware.TwoPhaseCommit;

import Middleware.CausalOrdering.VectorMessage;

public class TransactionMessage extends VectorMessage {
    private Identifier ident;
    private char response;

    public TransactionMessage(){
        super();
        this.ident = null;
        this.response = 'r';
    }

    public TransactionMessage(int transactionId, int serverId,  char response){
        super();
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

    @Override
    public String toString() {
        return "TransactionMessage{" + super.toString() +
                "ident=" + ident.toString() +
                ", response=" + response +
                '}';
    }

    public static void main(String[] args) {
        TransactionMessage tm = new TransactionMessage(1,1,'r') ;
        System.out.println(tm.toString());
    }
}
