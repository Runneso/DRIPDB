package ru.open.cu.student.sql.ast;

import java.util.Objects;

public record ColumnDef(SqlIdent name, String typeName) implements AstNode {
    public ColumnDef {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(typeName, "typeName");
    }
}


