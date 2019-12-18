package Logic;

import java.util.List;

public class Post {

    private int id;
    private String poster;
    private String text;
    private List<String> topics;

    public Post(int id, String poster, String text, List<String> topics) {
        this.id = id;
        this.poster = poster;
        this.text = text;
        this.topics = topics;
    }
}
