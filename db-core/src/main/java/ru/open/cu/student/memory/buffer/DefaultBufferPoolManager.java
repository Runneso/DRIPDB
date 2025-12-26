package ru.open.cu.student.memory.buffer;

import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.model.BufferSlot;
import ru.open.cu.student.memory.page.Page;
import ru.open.cu.student.memory.replacer.Replacer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public final class DefaultBufferPoolManager implements BufferPoolManager {
    private final int poolSize;
    private final PageFileManager pageFileManager;
    private final Replacer replacer;
    private final Path storageRoot;

    private final Map<PageKey, BufferSlot> pageTable = new HashMap<>();

    public DefaultBufferPoolManager(int poolSize, PageFileManager pageFileManager, Replacer replacer, Path storageRoot) {
        if (poolSize <= 0) throw new IllegalArgumentException("poolSize must be > 0");
        this.pageFileManager = Objects.requireNonNull(pageFileManager, "pageFileManager");
        this.replacer = Objects.requireNonNull(replacer, "replacer");
        this.storageRoot = Objects.requireNonNull(storageRoot, "storageRoot");
        this.poolSize = poolSize;
    }

    @Override
    public synchronized BufferSlot getPage(PageKey key) {
        Objects.requireNonNull(key, "key");

        BufferSlot slot = pageTable.get(key);
        if (slot != null) {
            slot.incrementUsage();
            touch(slot);
            return slot;
        }

        ensureSpace();
        Page page = pageFileManager.read(key.pageId(), storageRoot.resolve(key.fileId()));
        BufferSlot newSlot = new BufferSlot(key, page);
        pageTable.put(key, newSlot);
        touch(newSlot);
        return newSlot;
    }

    @Override
    public synchronized BufferSlot newPage(PageKey key, Page page) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(page, "page");
        if (pageTable.containsKey(key)) {
            throw new IllegalStateException("page already exists in buffer: " + key);
        }

        ensureSpace();
        BufferSlot slot = new BufferSlot(key, page);
        pageTable.put(key, slot);
        touch(slot);
        return slot;
    }

    @Override
    public synchronized void updatePage(PageKey key, Page page) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(page, "page");

        BufferSlot slot = pageTable.get(key);
        if (slot == null) {
            slot = getPage(key);
        }
        slot.setPage(page);
        slot.setDirty(true);
        touch(slot);
    }

    @Override
    public synchronized void pinPage(PageKey key) {
        Objects.requireNonNull(key, "key");

        BufferSlot slot = pageTable.get(key);
        if (slot == null) {
            slot = getPage(key);
        }
        slot.setPinned(true);
        replacer.delete(key);
    }

    @Override
    public synchronized void unpinPage(PageKey key) {
        Objects.requireNonNull(key, "key");

        BufferSlot slot = pageTable.get(key);
        if (slot == null || !slot.isPinned()) return;
        slot.setPinned(false);
        replacer.push(slot);
    }

    @Override
    public synchronized void flushPage(PageKey key) {
        Objects.requireNonNull(key, "key");

        BufferSlot slot = pageTable.get(key);
        if (slot == null) return;
        if (slot.isDirty()) {
            pageFileManager.write(slot.getPage(), storageRoot.resolve(key.fileId()));
            slot.setDirty(false);
        }
    }

    @Override
    public synchronized void flushAllPages() {
        for (Map.Entry<PageKey, BufferSlot> e : pageTable.entrySet()) {
            BufferSlot slot = e.getValue();
            if (slot.isDirty()) {
                pageFileManager.write(slot.getPage(), storageRoot.resolve(e.getKey().fileId()));
                slot.setDirty(false);
            }
        }
    }

    @Override
    public synchronized List<BufferSlot> getDirtyPages() {
        List<BufferSlot> res = new ArrayList<>();
        for (BufferSlot slot : pageTable.values()) {
            if (slot.isDirty()) res.add(slot);
        }
        return res;
    }

    private void touch(BufferSlot slot) {
        if (!slot.isPinned()) {
            replacer.push(slot);
        }
    }

    private void ensureSpace() {
        if (pageTable.size() < poolSize) return;

        BufferSlot victim = replacer.pickVictim();
        if (victim == null) {
            throw new IllegalStateException("No victim available (all pages pinned)");
        }

        if (victim.isDirty()) {
            PageKey key = victim.getKey();
            pageFileManager.write(victim.getPage(), storageRoot.resolve(key.fileId()));
            victim.setDirty(false);
        }

        pageTable.remove(victim.getKey());
    }
}


