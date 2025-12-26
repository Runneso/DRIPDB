package ru.open.cu.student.memory.replacer;

import org.junit.jupiter.api.Test;
import ru.open.cu.student.memory.buffer.PageKey;
import ru.open.cu.student.memory.model.BufferSlot;

import static org.junit.jupiter.api.Assertions.*;

class ClockReplacerTest {

    private static BufferSlot slot(String fileId, int pageId) {
        return new BufferSlot(new PageKey(fileId, pageId), null);
    }

    @Test
    void orderBasic() {
        ClockReplacer r = new ClockReplacer();
        r.push(slot("f", 1));
        r.push(slot("f", 2));
        r.push(slot("f", 3));
        assertEquals(new PageKey("f", 1), r.pickVictim().getKey());
        assertEquals(new PageKey("f", 2), r.pickVictim().getKey());
        assertEquals(new PageKey("f", 3), r.pickVictim().getKey());
        assertNull(r.pickVictim());
    }

    @Test
    void touchAffectsOrder() {
        ClockReplacer r = new ClockReplacer();
        BufferSlot s1 = slot("f", 1), s2 = slot("f", 2), s3 = slot("f", 3);
        r.push(s1);
        r.push(s2);
        r.push(s3);
        assertEquals(new PageKey("f", 1), r.pickVictim().getKey());
        r.push(s2);
        assertEquals(new PageKey("f", 3), r.pickVictim().getKey());
        assertEquals(new PageKey("f", 2), r.pickVictim().getKey());
        assertNull(r.pickVictim());
    }

    @Test
    void deleteRemoves() {
        ClockReplacer r = new ClockReplacer();
        r.push(slot("f", 1));
        r.push(slot("f", 2));
        r.delete(new PageKey("f", 2));
        assertEquals(new PageKey("f", 1), r.pickVictim().getKey());
        assertNull(r.pickVictim());
    }

    @Test
    void pinnedNotPushed() {
        ClockReplacer r = new ClockReplacer();
        BufferSlot pinned = slot("f", 10);
        pinned.setPinned(true);
        r.push(pinned);
        r.push(slot("f", 5));
        assertEquals(new PageKey("f", 5), r.pickVictim().getKey());
        assertNull(r.pickVictim());
    }

    @Test
    void emptyPickReturnsNull() {
        ClockReplacer r = new ClockReplacer();
        assertNull(r.pickVictim());
    }
}


