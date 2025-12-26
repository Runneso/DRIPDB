package ru.open.cu.student.memory.replacer;

import ru.open.cu.student.memory.buffer.PageKey;
import ru.open.cu.student.memory.model.BufferSlot;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public final class ClockReplacer implements Replacer {
    private final Deque<BufferSlot> ring = new ArrayDeque<>();
    private final Set<PageKey> inRing = new HashSet<>();
    private final Set<PageKey> ref = new HashSet<>();

    @Override
    public synchronized void push(BufferSlot slot) {
        if (slot == null) return;
        if (slot.isPinned()) return;
        PageKey key = slot.getKey();
        if (inRing.add(key)) ring.addLast(slot);
        ref.add(key);
    }

    @Override
    public synchronized void delete(PageKey key) {
        if (key == null) return;
        if (!inRing.remove(key)) return;
        ref.remove(key);
        for (Iterator<BufferSlot> it = ring.iterator(); it.hasNext(); ) {
            if (it.next().getKey().equals(key)) {
                it.remove();
                break;
            }
        }
    }

    @Override
    public synchronized BufferSlot pickVictim() {
        while (!ring.isEmpty()) {
            BufferSlot slot = ring.pollFirst();
            PageKey key = slot.getKey();
            if (ref.remove(key)) {
                ring.addLast(slot);
                continue;
            }
            inRing.remove(key);
            return slot;
        }
        return null;
    }
}


