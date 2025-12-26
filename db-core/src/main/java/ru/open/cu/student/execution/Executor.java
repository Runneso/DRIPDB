package ru.open.cu.student.execution;

import java.util.List;

public interface Executor {
    void open();

    
    List<Object> next();

    void close();
}


