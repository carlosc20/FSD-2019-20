package Middleware.TwoPhaseCommit;

public class TransactionMessage  {
    private int transactionId;
    private char type;
    private String name;

    public TransactionMessage(String name){
        this.transactionId = -1;
        this.type = 'b';
        this.name = name;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public char getType() {
        return type;
    }

    public void setType(char type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "TransactionMessage{" +
                " tId= " + this.transactionId +
                " type= " + this.type +
                '}';
    }
}
