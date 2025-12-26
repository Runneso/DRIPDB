package ru.open.cu.student.sql.ast;

import java.util.Objects;

public record ExplainStmt(Statement inner) implements Statement {
    public ExplainStmt {
        Objects.requireNonNull(inner, "inner");
    }
}


