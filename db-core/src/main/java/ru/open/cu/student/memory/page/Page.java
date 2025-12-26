package ru.open.cu.student.memory.page;

public interface Page {
    byte[] bytes();

    int getPageId();

    int size();

    boolean isValid();

    byte[] read(int index);

    void write(byte[] data);
}


