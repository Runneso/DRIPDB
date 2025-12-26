package ru.open.cu.student.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public final class DbRequest {
    public final String type;       
    public final String requestId;  
    public final String sql;
    public final boolean trace;

    @JsonCreator
    public DbRequest(
            @JsonProperty("type") String type,
            @JsonProperty("requestId") String requestId,
            @JsonProperty("sql") String sql,
            @JsonProperty("trace") boolean trace
    ) {
        this.type = type;
        this.requestId = requestId;
        this.sql = sql;
        this.trace = trace;
    }
}


