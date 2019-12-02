package Middleware.CausalOrdering;

import java.util.List;

public interface VectorOrdering {
    List<Integer> getVector();
    int getIndex(int index);
    int getId(); //posição do servidor na lista
    void setId(int id); //para não ter de andar com o id do servidor arrasto em todas as classes, porque o coh tem-o
    void setIndex(int index, int value);
    void setVector(List<Integer> counters);
}
