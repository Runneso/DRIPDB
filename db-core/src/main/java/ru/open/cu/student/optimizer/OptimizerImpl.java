package ru.open.cu.student.optimizer;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.optimizer.node.*;
import ru.open.cu.student.planner.node.*;
import ru.open.cu.student.sql.semantic.ResolvedBinaryExpr;
import ru.open.cu.student.sql.semantic.ResolvedColumnRef;
import ru.open.cu.student.sql.semantic.ResolvedConst;
import ru.open.cu.student.sql.semantic.ResolvedExpr;

import java.util.List;
import java.util.Objects;

public final class OptimizerImpl implements Optimizer {
    private final CatalogManager catalog;

    public OptimizerImpl(CatalogManager catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    public PhysicalPlanNode optimize(LogicalPlanNode logicalPlan) {
        Objects.requireNonNull(logicalPlan, "logicalPlan");

        if (logicalPlan instanceof ExplainNode ex) {
            return new PhysicalExplainNode(optimize(ex.inner()));
        }

        if (logicalPlan instanceof CreateTableNode ct) {
            return new PhysicalCreateTableNode(ct.query());
        }

        if (logicalPlan instanceof CreateIndexNode ci) {
            return new PhysicalCreateIndexNode(ci.query());
        }

        if (logicalPlan instanceof InsertNode ins) {
            return new PhysicalInsertNode(ins.query());
        }

        if (logicalPlan instanceof ProjectNode p) {
            return new PhysicalProjectNode(optimize(p.child()), p.columns());
        }

        if (logicalPlan instanceof FilterNode f) {
            
            if (f.child() instanceof ScanNode scan) {
                PhysicalPlanNode bestScan = chooseBestScan(scan.table(), f.predicate());
                if (!(bestScan instanceof PhysicalSeqScanNode)) {
                    
                    return new PhysicalFilterNode(bestScan, f.predicate());
                }
            }
            return new PhysicalFilterNode(optimize(f.child()), f.predicate());
        }

        if (logicalPlan instanceof ScanNode scan) {
            return new PhysicalSeqScanNode(scan.table());
        }

        throw new UnsupportedOperationException("Unsupported logical node: " + logicalPlan.getClass().getSimpleName());
    }

    private PhysicalPlanNode chooseBestScan(TableDefinition table, ResolvedExpr predicate) {
        
        Equality eq = extractEquality(predicate);
        if (eq != null) {
            IndexDefinition idx = findIndex(table, eq.column, IndexType.HASH);
            if (idx != null) {
                return new PhysicalHashIndexScanNode(table, idx, eq.value);
            }
        }

        
        Range range = extractRange(predicate);
        if (range != null) {
            IndexDefinition idx = findIndex(table, range.column, IndexType.BTREE);
            if (idx != null) {
                return new PhysicalBTreeIndexScanNode(
                        table,
                        idx,
                        range.from,
                        range.fromInclusive,
                        range.to,
                        range.toInclusive
                );
            }
        }

        return new PhysicalSeqScanNode(table);
    }

    private IndexDefinition findIndex(TableDefinition table, ColumnDefinition column, IndexType type) {
        List<IndexDefinition> indexes = catalog.listIndexes(table);
        for (IndexDefinition idx : indexes) {
            if (idx.getIndexType() == type && idx.getColumnOid() == column.getOid()) {
                return idx;
            }
        }
        return null;
    }

    private static final class Equality {
        final ColumnDefinition column;
        final Object value;

        Equality(ColumnDefinition column, Object value) {
            this.column = column;
            this.value = value;
        }
    }

    private static Equality extractEquality(ResolvedExpr predicate) {
        if (!(predicate instanceof ResolvedBinaryExpr b)) return null;
        if (!b.op().equals("=")) return null;

        if (b.left() instanceof ResolvedColumnRef c && b.right() instanceof ResolvedConst k) {
            return new Equality(c.column(), k.value());
        }
        if (b.right() instanceof ResolvedColumnRef c && b.left() instanceof ResolvedConst k) {
            return new Equality(c.column(), k.value());
        }
        return null;
    }

    private static final class Range {
        final ColumnDefinition column;
        Object from;
        boolean fromInclusive;
        Object to;
        boolean toInclusive;

        Range(ColumnDefinition column) {
            this.column = column;
        }
    }

    private static Range extractRange(ResolvedExpr predicate) {
        
        if (predicate instanceof ResolvedBinaryExpr b && b.op().equals("AND")) {
            Range left = extractRange(b.left());
            Range right = extractRange(b.right());
            if (left == null) return right;
            if (right == null) return left;
            if (left.column.getOid() != right.column.getOid()) return null;

            Range merged = new Range(left.column);
            
            if (left.from == null) {
                merged.from = right.from;
                merged.fromInclusive = right.fromInclusive;
            } else if (right.from == null) {
                merged.from = left.from;
                merged.fromInclusive = left.fromInclusive;
            } else {
                int c = cmp(left.from, right.from);
                if (c > 0) {
                    merged.from = left.from;
                    merged.fromInclusive = left.fromInclusive;
                } else if (c < 0) {
                    merged.from = right.from;
                    merged.fromInclusive = right.fromInclusive;
                } else {
                    merged.from = left.from;
                    merged.fromInclusive = left.fromInclusive && right.fromInclusive;
                }
            }

            
            if (left.to == null) {
                merged.to = right.to;
                merged.toInclusive = right.toInclusive;
            } else if (right.to == null) {
                merged.to = left.to;
                merged.toInclusive = left.toInclusive;
            } else {
                int c = cmp(left.to, right.to);
                if (c < 0) {
                    merged.to = left.to;
                    merged.toInclusive = left.toInclusive;
                } else if (c > 0) {
                    merged.to = right.to;
                    merged.toInclusive = right.toInclusive;
                } else {
                    merged.to = left.to;
                    merged.toInclusive = left.toInclusive && right.toInclusive;
                }
            }

            if (merged.from != null && merged.to != null && cmp(merged.from, merged.to) > 0) {
                return null;
            }
            return merged;
        }

        if (!(predicate instanceof ResolvedBinaryExpr b)) return null;
        String op = b.op();
        if (!isComparison(op)) return null;

        ColumnDefinition col;
        Object constant;
        String normOp = op;

        if (b.left() instanceof ResolvedColumnRef c && b.right() instanceof ResolvedConst k) {
            col = c.column();
            constant = k.value();
        } else if (b.right() instanceof ResolvedColumnRef c && b.left() instanceof ResolvedConst k) {
            col = c.column();
            constant = k.value();
            normOp = invert(op);
        } else {
            return null;
        }

        Range r = new Range(col);
        switch (normOp) {
            case "=" -> {
                r.from = constant;
                r.to = constant;
                r.fromInclusive = true;
                r.toInclusive = true;
            }
            case ">" -> {
                r.from = constant;
                r.fromInclusive = false;
            }
            case ">=" -> {
                r.from = constant;
                r.fromInclusive = true;
            }
            case "<" -> {
                r.to = constant;
                r.toInclusive = false;
            }
            case "<=" -> {
                r.to = constant;
                r.toInclusive = true;
            }
            default -> {
                return null;
            }
        }
        return r;
    }

    @SuppressWarnings("unchecked")
    private static int cmp(Object a, Object b) {
        return ((Comparable<Object>) a).compareTo(b);
    }

    private static boolean isComparison(String op) {
        return op.equals("=") || op.equals("<>") || op.equals("<") || op.equals("<=") || op.equals(">") || op.equals(">=");
    }

    private static String invert(String op) {
        return switch (op) {
            case "<" -> ">";
            case "<=" -> ">=";
            case ">" -> "<";
            case ">=" -> "<=";
            default -> op;
        };
    }
}


