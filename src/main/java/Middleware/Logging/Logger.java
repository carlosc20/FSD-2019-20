package Middleware.Logging;

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

    public Logger(String directory, String name, Serializer s) {
        this.sj = SegmentedJournal.builder()
                .withName(name)
                .withDirectory(directory)
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
}
