package Logic;


import java.util.ArrayList;
import java.util.List;

public class CircularArray<E> {

    private ArrayList<E> array;
    private int size;
    private int last;

    public CircularArray(int size) {
        this.size = size;
        this.array = new ArrayList<>(size);
        this.last = 0;
    }

    public List<E> getAll() {
        return array;
    }

    public void add(E element) {
        array.add(last, element);
        last = (last + 1) % size;
    }


}
