package Middleware;

import java.util.List;

public interface VectorOrdering {
    List<Integer> getVector();
    int getElement(int index);
    int getId(); //fica estranho
}
