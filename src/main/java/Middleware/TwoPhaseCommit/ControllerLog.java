package Middleware.TwoPhaseCommit;

public class ControllerLog {
    private int id; //id da transação
    private char type;
    //0 -> à espera de prepared por parte dos participantes
    //c -> commited
    //a -> aborted

    public ControllerLog(){
        this.id = 1;
        this.type = 0;
    }

    public void incr(){
        this.id++;
    }

    public void setCommited(){
        this.type = 'c';
    }

    public void setAborted(){
        this.type = 'a';
    }
}
