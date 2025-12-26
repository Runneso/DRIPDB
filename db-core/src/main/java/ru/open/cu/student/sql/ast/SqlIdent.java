package ru.open.cu.student.sql.ast;

import java.util.Objects;

public record SqlIdent(String text, int offset, int line, int column) {
    public SqlIdent {
        Objects.requireNonNull(text, "text");
    }
}


