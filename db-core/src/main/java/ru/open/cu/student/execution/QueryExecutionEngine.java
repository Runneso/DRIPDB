package ru.open.cu.student.execution;

import java.util.List;

public interface QueryExecutionEngine {
    List<List<Object>> execute(Executor executor);
}


