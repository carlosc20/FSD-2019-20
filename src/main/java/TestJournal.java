import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

public class TestJournal {
    public static void main(String[] args) throws Exception {
        Serializer s = Serializer.builder()
                .build();

        SegmentedJournal<String> sj = SegmentedJournal.<String>builder()
                .withName("exemplo")
                .withSerializer(s)
                .build();

        // Arranque

        SegmentedJournalReader<String> r = sj.openReader(2);
        while(r.hasNext()) {
            Indexed<String> e = r.next();
            System.out.println(e.index()+": "+e.entry());
        }
        r.close();

        // Funcionamento normal

        SegmentedJournalWriter<String> w = sj.writer();
        w.append("ola");
        CompletableFuture.supplyAsync(()->{w.flush();return null;})
                .thenRun(()->{
                    w.close();
                });

    }
}
