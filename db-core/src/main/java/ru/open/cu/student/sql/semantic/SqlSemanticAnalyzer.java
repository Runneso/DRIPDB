package ru.open.cu.student.sql.semantic;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.sql.ast.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class SqlSemanticAnalyzer {

    public QueryTree analyze(Statement ast, CatalogManager catalog) {
        Objects.requireNonNull(ast, "ast");
        Objects.requireNonNull(catalog, "catalog");

        if (ast instanceof ExplainStmt ex) {
            return new ExplainQueryTree(analyze(ex.inner(), catalog));
        }
        if (ast instanceof CreateTableStmt ct) {
            return analyzeCreateTable(ct, catalog);
        }
        if (ast instanceof InsertStmt ins) {
            return analyzeInsert(ins, catalog);
        }
        if (ast instanceof SelectStmt sel) {
            return analyzeSelect(sel, catalog);
        }
        if (ast instanceof CreateIndexStmt ci) {
            return analyzeCreateIndex(ci, catalog);
        }

        throw new SqlSemanticException("Unsupported statement type: " + ast.getClass().getSimpleName(), null, null, null);
    }

    private QueryTree analyzeCreateTable(CreateTableStmt stmt, CatalogManager catalog) {
        SqlIdent tableName = stmt.tableName();
        if (catalog.getTable(tableName.text()) != null) {
            throw semanticError("Table already exists: " + tableName.text(), tableName);
        }

        Set<String> seen = new HashSet<>();
        List<CreateTableQueryTree.ResolvedCreateColumn> cols = new ArrayList<>();

        for (ColumnDef cd : stmt.columns()) {
            String colName = cd.name().text();
            if (!seen.add(colName)) {
                throw semanticError("Duplicate column: " + colName, cd.name());
            }

            String typeName = normalizeTypeName(cd.typeName());
            TypeDefinition type = catalog.getTypeByName(typeName);
            if (type == null) {
                throw semanticError("Unknown type: " + cd.typeName(), cd.name());
            }

            cols.add(new CreateTableQueryTree.ResolvedCreateColumn(cd.name(), type));
        }

        return new CreateTableQueryTree(tableName, cols);
    }

    private QueryTree analyzeInsert(InsertStmt stmt, CatalogManager catalog) {
        SqlIdent tableName = stmt.tableName();
        TableDefinition table = catalog.getTable(tableName.text());
        if (table == null) {
            throw semanticError("Table not found: " + tableName.text(), tableName);
        }

        List<ColumnDefinition> columns = catalog.getColumns(table);
        if (stmt.values().size() != columns.size()) {
            throw semanticError("Values count mismatch: expected " + columns.size() + " got " + stmt.values().size(), tableName);
        }

        List<Object> values = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition col = columns.get(i);
            TypeDefinition type = catalog.getTypeByOid(col.getTypeOid());
            if (type == null) {
                throw new SqlSemanticException("Unknown type oid: " + col.getTypeOid(), null, null, null);
            }

            Expr expr = stmt.values().get(i);
            Object v = resolveLiteral(expr);
            ExprType vt = inferLiteralType(expr);
            ExprType ct = toExprType(type);

            if (vt != ct) {
                throw new SqlSemanticException(
                        "Type mismatch for column " + col.getName() + ": expected " + ct + " got " + vt,
                        null, null, null
                );
            }

            values.add(v);
        }

        return new InsertQueryTree(table, columns, values);
    }

    private QueryTree analyzeSelect(SelectStmt stmt, CatalogManager catalog) {
        SqlIdent tableName = stmt.tableName();
        TableDefinition table = catalog.getTable(tableName.text());
        if (table == null) {
            throw semanticError("Table not found: " + tableName.text(), tableName);
        }

        List<ColumnDefinition> outCols = new ArrayList<>();
        if (stmt.selectAll()) {
            outCols.addAll(catalog.getColumns(table));
        } else {
            for (SqlIdent colName : stmt.columns()) {
                ColumnDefinition col = catalog.getColumn(table, colName.text());
                if (col == null) {
                    throw semanticError("Column not found: " + tableName.text() + "." + colName.text(), colName);
                }
                outCols.add(col);
            }
        }

        ResolvedExpr filter = null;
        if (stmt.where() != null) {
            filter = resolveExpr(stmt.where(), table, catalog);
            if (filter.getExprType() != ExprType.BOOL) {
                throw new SqlSemanticException("WHERE clause must be boolean", null, null, null);
            }
        }

        return new SelectQueryTree(table, outCols, filter);
    }

    private QueryTree analyzeCreateIndex(CreateIndexStmt stmt, CatalogManager catalog) {
        SqlIdent indexName = stmt.indexName();
        if (catalog.getIndex(indexName.text()) != null) {
            throw semanticError("Index already exists: " + indexName.text(), indexName);
        }

        SqlIdent tableName = stmt.tableName();
        TableDefinition table = catalog.getTable(tableName.text());
        if (table == null) {
            throw semanticError("Table not found: " + tableName.text(), tableName);
        }

        SqlIdent columnName = stmt.columnName();
        ColumnDefinition col = catalog.getColumn(table, columnName.text());
        if (col == null) {
            throw semanticError("Column not found: " + tableName.text() + "." + columnName.text(), columnName);
        }

        IndexType indexType = stmt.indexType();
        return new CreateIndexQueryTree(indexName, table, col, indexType);
    }

    private ResolvedExpr resolveExpr(Expr expr, TableDefinition table, CatalogManager catalog) {
        if (expr instanceof ColumnRefExpr cr) {
            SqlIdent name = cr.name();
            ColumnDefinition col = catalog.getColumn(table, name.text());
            if (col == null) {
                throw semanticError("Column not found: " + table.getName() + "." + name.text(), name);
            }
            TypeDefinition t = catalog.getTypeByOid(col.getTypeOid());
            if (t == null) {
                throw new SqlSemanticException("Unknown type oid: " + col.getTypeOid(), null, null, null);
            }
            return new ResolvedColumnRef(col, toExprType(t));
        }

        if (expr instanceof LiteralInt64Expr li) {
            return new ResolvedConst(li.value(), ExprType.INT64);
        }
        if (expr instanceof LiteralStringExpr ls) {
            return new ResolvedConst(ls.value(), ExprType.VARCHAR);
        }

        if (expr instanceof BinaryExpr b) {
            ResolvedExpr left = resolveExpr(b.left(), table, catalog);
            ResolvedExpr right = resolveExpr(b.right(), table, catalog);
            String op = b.op();

            if (op.equals("AND") || op.equals("OR")) {
                requireType(left, ExprType.BOOL, "Left operand of " + op);
                requireType(right, ExprType.BOOL, "Right operand of " + op);
                return new ResolvedBinaryExpr(op, left, right, ExprType.BOOL);
            }

            
            if (isComparison(op)) {
                if (left.getExprType() != right.getExprType()) {
                    throw new SqlSemanticException("Type mismatch in comparison: " + left.getExprType() + " vs " + right.getExprType(), null, null, null);
                }
                if (left.getExprType() == ExprType.BOOL) {
                    throw new SqlSemanticException("Cannot compare BOOL values", null, null, null);
                }
                return new ResolvedBinaryExpr(op, left, right, ExprType.BOOL);
            }

            throw new SqlSemanticException("Unsupported operator: " + op, null, null, null);
        }

        throw new SqlSemanticException("Unsupported expression type: " + expr.getClass().getSimpleName(), null, null, null);
    }

    private static boolean isComparison(String op) {
        return op.equals("=") || op.equals("<>") || op.equals("<") || op.equals("<=") || op.equals(">") || op.equals(">=");
    }

    private static void requireType(ResolvedExpr expr, ExprType expected, String what) {
        if (expr.getExprType() != expected) {
            throw new SqlSemanticException(what + " must be " + expected + ", got " + expr.getExprType(), null, null, null);
        }
    }

    private static Object resolveLiteral(Expr expr) {
        if (expr instanceof LiteralInt64Expr li) {
            return li.value();
        }
        if (expr instanceof LiteralStringExpr ls) {
            return ls.value();
        }
        throw new SqlSemanticException("Only literal values are supported in INSERT", null, null, null);
    }

    private static ExprType inferLiteralType(Expr expr) {
        if (expr instanceof LiteralInt64Expr) return ExprType.INT64;
        if (expr instanceof LiteralStringExpr) return ExprType.VARCHAR;
        throw new SqlSemanticException("Only literal values are supported", null, null, null);
    }

    private static ExprType toExprType(TypeDefinition type) {
        return switch (type.getName()) {
            case "INT64" -> ExprType.INT64;
            case "VARCHAR" -> ExprType.VARCHAR;
            default -> throw new SqlSemanticException("Unsupported type: " + type.getName(), null, null, null);
        };
    }

    private static SqlSemanticException semanticError(String message, SqlIdent ident) {
        return new SqlSemanticException(message, ident.offset(), ident.line(), ident.column());
    }

    private static String normalizeTypeName(String typeName) {
        if (typeName == null) return null;
        String upper = typeName.toUpperCase();
        return switch (upper) {
            case "INT", "INTEGER" -> "INT64";
            default -> upper;
        };
    }
}


