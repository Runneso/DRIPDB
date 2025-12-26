package ru.open.cu.student.execution;

import java.util.ArrayList;
import java.util.List;

public final class QueryExecutionEngineImpl implements QueryExecutionEngine {
    @Override
    public List<List<Object>> execute(Executor executor) {
        if (executor == null) {
            throw new IllegalArgumentException("executor is null");
        }

        List<List<Object>> rows = new ArrayList<>();
        try {
            executor.open();
            List<Object> row;
            while ((row = executor.next()) != null) {
                rows.add(row);
            }
        } finally {
            executor.close();
        }
        return rows;
    }
}


