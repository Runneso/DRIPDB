package ru.open.cu.student.memory.replacer;

import org.junit.jupiter.api.Test;
import ru.open.cu.student.memory.buffer.PageKey;
import ru.open.cu.student.memory.model.BufferSlot;

import static org.junit.jupiter.api.Assertions.*;

public class LRUReplacerTest {

    private static BufferSlot slot(String fileId, int pageId) {
        return new BufferSlot(new PageKey(fileId, pageId), null);
    }

    @Test
    void orderBasic() {
        LRUReplacer r = new LRUReplacer();
        BufferSlot s1 = slot("f", 1);
        BufferSlot s2 = slot("f", 2);
        BufferSlot s3 = slot("f", 3);
        r.push(s1);
        r.push(s2);
        r.push(s3);
        assertEquals(new PageKey("f", 1), r.pickVictim().getKey());
        assertEquals(new PageKey("f", 2), r.pickVictim().getKey());
        assertEquals(new PageKey("f", 3), r.pickVictim().getKey());
        assertNull(r.pickVictim());
    }

    @Test
    void recencyAfterPushAgain() {
        LRUReplacer r = new LRUReplacer();
        BufferSlot s1 = slot("f", 1);
        BufferSlot s2 = slot("f", 2);
        BufferSlot s3 = slot("f", 3);
        r.push(s1);
        r.push(s2);
        r.push(s3);
        r.push(s2);
        assertEquals(new PageKey("f", 1), r.pickVictim().getKey());
        assertEquals(new PageKey("f", 3), r.pickVictim().getKey());
        assertEquals(new PageKey("f", 2), r.pickVictim().getKey());
        assertNull(r.pickVictim());
    }

    @Test
    void deleteRemoves() {
        LRUReplacer r = new LRUReplacer();
        BufferSlot s1 = slot("f", 1);
        BufferSlot s2 = slot("f", 2);
        r.push(s1);
        r.push(s2);
        r.delete(new PageKey("f", 1));
        assertEquals(new PageKey("f", 2), r.pickVictim().getKey());
        assertNull(r.pickVictim());
    }

    @Test
    void pinnedNotPushed() {
        LRUReplacer r = new LRUReplacer();
        BufferSlot pinned = slot("f", 10);
        pinned.setPinned(true);
        BufferSlot s = slot("f", 5);
        r.push(pinned);
        r.push(s);
        assertEquals(new PageKey("f", 5), r.pickVictim().getKey());
        assertNull(r.pickVictim());
    }

    @Test
    void emptyPickReturnsNull() {
        LRUReplacer r = new LRUReplacer();
        assertNull(r.pickVictim());
    }
}


