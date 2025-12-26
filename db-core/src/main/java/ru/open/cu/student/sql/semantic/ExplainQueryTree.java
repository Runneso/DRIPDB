package ru.open.cu.student.sql.semantic;

import java.util.Objects;

public record ExplainQueryTree(QueryTree inner) implements QueryTree {
    public ExplainQueryTree {
        Objects.requireNonNull(inner, "inner");
    }

    @Override
    public QueryType getType() {
        return QueryType.EXPLAIN;
    }
}


