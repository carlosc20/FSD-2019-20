package Logic;

import java.util.Arrays;
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

    public int getId() {
        return id;
    }

    public String getPoster() {
        return poster;
    }

    public String getText() {
        return text;
    }

    public List<String> getTopics() {
        return topics;
    }

    @Override
    public String toString() {
        return text + "\npublicado por " + poster + " em " + Arrays.toString(topics.toArray()) + "\n---";
    }
}
