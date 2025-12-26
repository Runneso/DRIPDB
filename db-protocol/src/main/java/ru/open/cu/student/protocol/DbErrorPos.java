package ru.open.cu.student.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class DbErrorPos {
    public final Integer offset;
    public final Integer line;
    public final Integer column;

    @JsonCreator
    public DbErrorPos(
            @JsonProperty("offset") Integer offset,
            @JsonProperty("line") Integer line,
            @JsonProperty("column") Integer column
    ) {
        this.offset = offset;
        this.line = line;
        this.column = column;
    }
}


