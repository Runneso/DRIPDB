package ru.open.cu.student.memory.io;

public interface DirtyPageWriter {
    void startBackgroundWriter();

    void startCheckPointer();
}


