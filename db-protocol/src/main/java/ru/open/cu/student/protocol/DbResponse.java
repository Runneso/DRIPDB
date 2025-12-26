package ru.open.cu.student.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class DbResponse {
    public final String requestId;
    public final String status; 

    public final List<String> columns;
    public final List<List<Object>> rows;
    public final Integer affected;
    public final String explain;

    public final DbError error;

    @JsonCreator
    public DbResponse(
            @JsonProperty("requestId") String requestId,
            @JsonProperty("status") String status,
            @JsonProperty("columns") List<String> columns,
            @JsonProperty("rows") List<List<Object>> rows,
            @JsonProperty("affected") Integer affected,
            @JsonProperty("explain") String explain,
            @JsonProperty("error") DbError error
    ) {
        this.requestId = requestId;
        this.status = status;
        this.columns = columns;
        this.rows = rows;
        this.affected = affected;
        this.explain = explain;
        this.error = error;
    }

    public static DbResponse ok(String requestId, List<String> columns, List<List<Object>> rows, Integer affected, String explain) {
        return new DbResponse(requestId, "ok", columns, rows, affected, explain, null);
    }

    public static DbResponse error(String requestId, DbError error) {
        return new DbResponse(requestId, "error", null, null, null, null, error);
    }
}


