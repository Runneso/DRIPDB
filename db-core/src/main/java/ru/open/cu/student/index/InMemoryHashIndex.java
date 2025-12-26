package ru.open.cu.student.index;

import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.storage.TID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class InMemoryHashIndex implements Index {
    private final IndexDefinition def;
    private final Map<Comparable<?>, List<TID>> map = new HashMap<>();

    public InMemoryHashIndex(IndexDefinition def) {
        this.def = Objects.requireNonNull(def, "def");
        if (def.getIndexType() != IndexType.HASH) {
            throw new IllegalArgumentException("Not a HASH index definition: " + def.getIndexType());
        }
    }

    @Override
    public IndexDefinition getDefinition() {
        return def;
    }

    @Override
    public synchronized void insert(Comparable<?> key, TID tid) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(tid, "tid");
        map.computeIfAbsent(key, ignored -> new ArrayList<>()).add(tid);
    }

    @Override
    public synchronized List<TID> search(Comparable<?> key) {
        Objects.requireNonNull(key, "key");
        List<TID> list = map.get(key);
        if (list == null) return List.of();
        return List.copyOf(list);
    }

    @Override
    public List<TID> rangeSearch(Comparable<?> from, boolean fromInclusive, Comparable<?> to, boolean toInclusive) {
        throw new UnsupportedOperationException("HASH index does not support rangeSearch");
    }
}


