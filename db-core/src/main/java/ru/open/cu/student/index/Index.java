package ru.open.cu.student.index;

import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.storage.TID;

import java.util.List;

public interface Index {
    IndexDefinition getDefinition();

    void insert(Comparable<?> key, TID tid);

    List<TID> search(Comparable<?> key);

    
    List<TID> rangeSearch(Comparable<?> from, boolean fromInclusive, Comparable<?> to, boolean toInclusive);
}


