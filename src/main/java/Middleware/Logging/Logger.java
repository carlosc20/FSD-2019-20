package Middleware.Logging;

import Middleware.CausalOrdering.VectorMessage;
import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

public class Logger {
    private SegmentedJournal<Object> sj;
    private SegmentedJournalWriter<Object> w;
    private SegmentedJournalReader<Object> r;

    public Logger(String name, Serializer s) {
        this.sj = SegmentedJournal.builder()
                .withName(name)
                .withSerializer(s)
                .build();
        this.w = sj.writer();
    }

    public void write(Object o){
        w.append(o);
        CompletableFuture.supplyAsync(() -> {
            w.flush();
            return null;
        }).thenRun(() -> {
            w.close();
        });
    }

    public ArrayList<Object> recover() {
        ArrayList<Object> actions = new ArrayList<>();
        r = sj.openReader(0);
        while (r.hasNext()) {
            Indexed<Object> e = r.next();
            actions.add(e.entry());
            System.out.println(e.index()+": "+e.entry());
        }
        r.close();
        return actions;
    }

    public ArrayList<Object> recover(int index) {
        ArrayList<Object> actions = new ArrayList<>();
        r = sj.openReader(index);
        while (r.hasNext()) {
            Indexed<Object> e = r.next();
            actions.add(e.entry());
        }
        r.close();
        return actions;
    }

    public static void main(String[] args) {
        Serializer s = VectorMessage.serializer;
        Logger l = new Logger("teste", s);
        VectorMessage vm = new VectorMessage(1,"tudo bem??? beijo!", 2);
        vm.setIndex(0,3);
        l.write(vm);
        ArrayList<Object> messages = l.recover();
        for(int i = 0; i<messages.size(); i++){
            VectorMessage m = (VectorMessage) messages.get(i);
            m.toString();
        }
    }
}
