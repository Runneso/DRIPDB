package ru.open.cu.student.memory.buffer;

import org.junit.jupiter.api.Test;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.model.BufferSlot;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;
import ru.open.cu.student.memory.replacer.Replacer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DefaultBufferPoolManagerTest {

    static class FakePFM implements PageFileManager {
        int readCount = 0;
        int writeCount = 0;

        @Override
        public void write(Page page, Path path) {
            writeCount++;
        }

        @Override
        public Page read(int pageId, Path path) {
            readCount++;
            return new HeapPage(pageId);
        }
    }

    static class FIFOReplacer implements Replacer {
        private final Deque<BufferSlot> q = new ArrayDeque<>();

        @Override
        public synchronized void push(BufferSlot s) {
            if (s == null || s.isPinned()) return;
            for (BufferSlot x : q) if (x.getKey().equals(s.getKey())) return;
            q.addLast(s);
        }

        @Override
        public synchronized void delete(PageKey key) {
            if (key == null) return;
            q.removeIf(s -> s.getKey().equals(key));
        }

        @Override
        public synchronized BufferSlot pickVictim() {
            while (!q.isEmpty()) {
                BufferSlot s = q.pollFirst();
                if (s.isPinned()) continue;
                return s;
            }
            return null;
        }
    }

    private DefaultBufferPoolManager newManager(int poolSize, FakePFM pfm) {
        return new DefaultBufferPoolManager(poolSize, pfm, new FIFOReplacer(), Paths.get("build/tmp/pages"));
    }

    @Test
    void testGetPageDoesNotReReadOnHit() {
        FakePFM pfm = new FakePFM();
        DefaultBufferPoolManager m = newManager(2, pfm);
        m.getPage(new PageKey("a.dat", 1));
        m.getPage(new PageKey("a.dat", 1));
        assertEquals(1, pfm.readCount);
    }

    @Test
    void testEvictionFlushesDirtyPage() {
        FakePFM pfm = new FakePFM();
        DefaultBufferPoolManager m = newManager(1, pfm);
        PageKey p1 = new PageKey("a.dat", 1);
        PageKey p2 = new PageKey("a.dat", 2);

        m.getPage(p1);
        m.updatePage(p1, new HeapPage(1));
        m.getPage(p2);

        assertEquals(1, pfm.writeCount);
        assertEquals(2, pfm.readCount);
    }

    @Test
    void testPinPreventsEviction() {
        FakePFM pfm = new FakePFM();
        DefaultBufferPoolManager m = newManager(1, pfm);
        PageKey p1 = new PageKey("a.dat", 1);
        PageKey p2 = new PageKey("a.dat", 2);

        m.getPage(p1);
        m.pinPage(p1);
        assertThrows(IllegalStateException.class, () -> m.getPage(p2));
    }

    @Test
    void testFlushPageWritesOnceAndClearsDirty() {
        FakePFM pfm = new FakePFM();
        DefaultBufferPoolManager m = newManager(2, pfm);
        PageKey p1 = new PageKey("a.dat", 1);

        m.getPage(p1);
        m.updatePage(p1, new HeapPage(1));
        m.flushPage(p1);
        assertEquals(1, pfm.writeCount);
        m.flushPage(p1);
        assertEquals(1, pfm.writeCount);
    }

    @Test
    void testFlushAllPagesWritesAllDirtyOnce() {
        FakePFM pfm = new FakePFM();
        DefaultBufferPoolManager m = newManager(2, pfm);
        PageKey p1 = new PageKey("a.dat", 1);
        PageKey p2 = new PageKey("a.dat", 2);

        m.getPage(p1);
        m.updatePage(p1, new HeapPage(1));
        m.getPage(p2);
        m.updatePage(p2, new HeapPage(2));
        m.flushAllPages();

        assertEquals(2, pfm.writeCount);
        m.flushAllPages();
        assertEquals(2, pfm.writeCount);
    }

    @Test
    void testGetDirtyPagesReturnsOnlyDirty() {
        FakePFM pfm = new FakePFM();
        DefaultBufferPoolManager m = newManager(2, pfm);
        PageKey p1 = new PageKey("a.dat", 1);
        PageKey p2 = new PageKey("a.dat", 2);

        m.getPage(p1);
        m.updatePage(p1, new HeapPage(1));
        m.getPage(p2);

        List<BufferSlot> dirty = m.getDirtyPages();
        Set<PageKey> keys = dirty.stream().map(BufferSlot::getKey).collect(Collectors.toSet());
        assertTrue(keys.contains(p1));
        assertFalse(keys.contains(p2));
    }
}


