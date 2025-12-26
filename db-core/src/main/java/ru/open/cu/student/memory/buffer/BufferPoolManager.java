package ru.open.cu.student.memory.buffer;

import ru.open.cu.student.memory.model.BufferSlot;
import ru.open.cu.student.memory.page.Page;

import java.util.List;

public interface BufferPoolManager {
    BufferSlot getPage(PageKey key);

    
    BufferSlot newPage(PageKey key, Page page);

    void updatePage(PageKey key, Page page);

    void pinPage(PageKey key);

    void unpinPage(PageKey key);

    void flushPage(PageKey key);

    void flushAllPages();

    List<BufferSlot> getDirtyPages();
}


