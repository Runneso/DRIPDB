package ru.open.cu.student.memory.replacer;

import ru.open.cu.student.memory.buffer.PageKey;
import ru.open.cu.student.memory.model.BufferSlot;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class LRUReplacer implements Replacer {
    private static final int DEFAULT_CAPACITY = 16;
    private static final float LOAD_FACTOR = 0.75f;

    private final LinkedHashMap<PageKey, BufferSlot> lru = new LinkedHashMap<>(DEFAULT_CAPACITY, LOAD_FACTOR, true);

    @Override
    public synchronized void push(BufferSlot bufferSlot) {
        if (bufferSlot == null) return;
        if (bufferSlot.isPinned()) return;
        lru.put(bufferSlot.getKey(), bufferSlot);
    }

    @Override
    public synchronized void delete(PageKey key) {
        if (key == null) return;
        lru.remove(key);
    }

    @Override
    public synchronized BufferSlot pickVictim() {
        Iterator<Map.Entry<PageKey, BufferSlot>> it = lru.entrySet().iterator();
        if (it.hasNext()) {
            Map.Entry<PageKey, BufferSlot> entry = it.next();
            BufferSlot slot = entry.getValue();
            it.remove();
            return slot;
        }
        return null;
    }
}


