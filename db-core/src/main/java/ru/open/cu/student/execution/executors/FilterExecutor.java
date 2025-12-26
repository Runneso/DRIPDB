package ru.open.cu.student.execution.executors;

import ru.open.cu.student.execution.Executor;
import ru.open.cu.student.sql.semantic.ResolvedBinaryExpr;
import ru.open.cu.student.sql.semantic.ResolvedColumnRef;
import ru.open.cu.student.sql.semantic.ResolvedConst;
import ru.open.cu.student.sql.semantic.ResolvedExpr;

import java.util.List;
import java.util.Objects;

public final class FilterExecutor implements Executor {
    private final Executor child;
    private final ResolvedExpr predicate;

    private boolean isOpen;

    public FilterExecutor(Executor child, ResolvedExpr predicate) {
        this.child = Objects.requireNonNull(child, "child");
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    @Override
    public void open() {
        child.open();
        isOpen = true;
    }

    @Override
    public List<Object> next() {
        if (!isOpen) throw new IllegalStateException("Executor is not open");

        while (true) {
            List<Object> row = child.next();
            if (row == null) return null;
            if (evalBool(predicate, row)) {
                return row;
            }
        }
    }

    @Override
    public void close() {
        isOpen = false;
        child.close();
    }

    private boolean evalBool(ResolvedExpr expr, List<Object> row) {
        Object v = eval(expr, row);
        if (!(v instanceof Boolean b)) {
            throw new IllegalStateException("Predicate did not evaluate to boolean: " + v);
        }
        return b;
    }

    private Object eval(ResolvedExpr expr, List<Object> row) {
        if (expr instanceof ResolvedConst c) {
            return c.value();
        }
        if (expr instanceof ResolvedColumnRef c) {
            return row.get(c.column().getPosition());
        }
        if (expr instanceof ResolvedBinaryExpr b) {
            String op = b.op();
            if (op.equals("AND")) {
                return evalBool(b.left(), row) && evalBool(b.right(), row);
            }
            if (op.equals("OR")) {
                return evalBool(b.left(), row) || evalBool(b.right(), row);
            }

            Object l = eval(b.left(), row);
            Object r = eval(b.right(), row);

            return switch (op) {
                case "=" -> Objects.equals(l, r);
                case "<>" -> !Objects.equals(l, r);
                case "<" -> cmp(l, r) < 0;
                case "<=" -> cmp(l, r) <= 0;
                case ">" -> cmp(l, r) > 0;
                case ">=" -> cmp(l, r) >= 0;
                default -> throw new IllegalStateException("Unsupported operator: " + op);
            };
        }
        throw new IllegalStateException("Unsupported resolved expr: " + expr.getClass().getSimpleName());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int cmp(Object a, Object b) {
        if (a == null || b == null) {
            throw new IllegalStateException("Cannot compare nulls");
        }
        if (!(a instanceof Comparable ca)) {
            throw new IllegalStateException("Not comparable: " + a);
        }
        return ca.compareTo(b);
    }
}


