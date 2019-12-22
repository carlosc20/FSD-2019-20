package Middleware.Logging;

import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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
        }).thenRun(() -> w.close());
    }

    public void recover(Consumer<Object> callback) {
        r = sj.openReader(0);
        while (r.hasNext()) {
            Indexed<Object> e = r.next();
            callback.accept(e.entry());
            System.out.println("Retrived from loggs " + e.index()+": "+e.entry());
        }
        r.close();
    }

    public Object recoverLast() {
        Object o = null;
        int last = sj.maxEntrySize()-1;
        r = sj.openReader(last);
        while (r.hasNext()) {
            Indexed<Object> e = r.next();
            o = e.entry();
            System.out.println("Retrived from loggs " + e.index()+": "+e.entry());
        }
        r.close();
        return o;
    }
}
