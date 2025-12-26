package ru.open.cu.student.index;

import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.storage.TID;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public final class InMemoryBTreeIndex implements Index {
    private final IndexDefinition def;
    private final NavigableMap<Comparable<?>, List<TID>> map = new TreeMap<>();

    public InMemoryBTreeIndex(IndexDefinition def) {
        this.def = Objects.requireNonNull(def, "def");
        if (def.getIndexType() != IndexType.BTREE) {
            throw new IllegalArgumentException("Not a BTREE index definition: " + def.getIndexType());
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
    public synchronized List<TID> rangeSearch(Comparable<?> from, boolean fromInclusive, Comparable<?> to, boolean toInclusive) {
        NavigableMap<Comparable<?>, List<TID>> sub;
        if (from == null && to == null) {
            sub = map;
        } else if (from == null) {
            sub = map.headMap(to, toInclusive);
        } else if (to == null) {
            sub = map.tailMap(from, fromInclusive);
        } else {
            sub = map.subMap(from, fromInclusive, to, toInclusive);
        }

        List<TID> out = new ArrayList<>();
        for (List<TID> tids : sub.values()) {
            out.addAll(tids);
        }
        return out;
    }
}


