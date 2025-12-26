package ru.open.cu.student.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class DbError {
    public final String code;    
    public final String message;
    public final DbErrorPos pos;

    @JsonCreator
    public DbError(
            @JsonProperty("code") String code,
            @JsonProperty("message") String message,
            @JsonProperty("pos") DbErrorPos pos
    ) {
        this.code = code;
        this.message = message;
        this.pos = pos;
    }
}


