package ru.open.cu.student.engine;


public record SessionContext(String sessionId, String requestId, boolean trace) {
    public SessionContext {
        if (sessionId != null && sessionId.isBlank()) {
            throw new IllegalArgumentException("sessionId is blank");
        }
        if (requestId != null && requestId.isBlank()) {
            throw new IllegalArgumentException("requestId is blank");
        }
    }
}


