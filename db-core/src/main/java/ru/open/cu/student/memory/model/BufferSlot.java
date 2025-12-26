package ru.open.cu.student.memory.model;

import ru.open.cu.student.memory.buffer.PageKey;
import ru.open.cu.student.memory.page.Page;

import java.util.Objects;

public final class BufferSlot {
    private final PageKey key;
    private Page page;
    private boolean dirty;
    private boolean pinned;
    private int usageCount;

    public BufferSlot(PageKey key, Page page) {
        this.key = Objects.requireNonNull(key, "key");
        this.page = page;
        this.dirty = false;
        this.pinned = false;
        this.usageCount = 0;
    }

    public PageKey getKey() {
        return key;
    }

    public Page getPage() {
        return page;
    }

    public void setPage(Page page) {
        this.page = page;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isPinned() {
        return pinned;
    }

    public void setPinned(boolean pinned) {
        this.pinned = pinned;
    }

    public int getUsageCount() {
        return usageCount;
    }

    public void incrementUsage() {
        this.usageCount++;
    }

    @Override
    public String toString() {
        return "BufferSlot{" +
                "key=" + key +
                ", dirty=" + dirty +
                ", pinned=" + pinned +
                ", usageCount=" + usageCount +
                '}';
    }
}


