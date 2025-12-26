package ru.open.cu.student.memory.replacer;

import ru.open.cu.student.memory.buffer.PageKey;
import ru.open.cu.student.memory.model.BufferSlot;

public interface Replacer {
    void push(BufferSlot bufferSlot);

    void delete(PageKey key);

    BufferSlot pickVictim();
}


