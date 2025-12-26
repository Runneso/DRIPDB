package ru.open.cu.student.engine;

import java.util.List;

public record ExecutionResult(
        List<String> columns,
        List<List<Object>> rows,
        int affected,
        String explain
) {
}


