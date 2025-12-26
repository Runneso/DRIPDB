package ru.open.cu.student.engine;

import ru.open.cu.student.optimizer.node.PhysicalPlanNode;
import ru.open.cu.student.planner.node.LogicalPlanNode;
import ru.open.cu.student.sql.ast.Statement;
import ru.open.cu.student.sql.lexer.Token;
import ru.open.cu.student.sql.semantic.QueryTree;

import java.util.List;
import java.util.Objects;

public final class ExplainFormatter {
    private ExplainFormatter() {
    }

    public static String format(
            List<Token> tokens,
            Statement ast,
            QueryTree queryTree,
            LogicalPlanNode logicalPlan,
            PhysicalPlanNode physicalPlan
    ) {
        Objects.requireNonNull(tokens, "tokens");
        Objects.requireNonNull(ast, "ast");
        Objects.requireNonNull(queryTree, "queryTree");
        Objects.requireNonNull(logicalPlan, "logicalPlan");
        Objects.requireNonNull(physicalPlan, "physicalPlan");

        StringBuilder sb = new StringBuilder();
        sb.append("TOKENS:\n");
        for (Token t : tokens) {
            sb.append("  ").append(t).append('\n');
        }

        sb.append("\nAST:\n");
        sb.append(ast).append('\n');

        sb.append("\nQUERY_TREE:\n");
        sb.append(queryTree).append('\n');

        sb.append("\nLOGICAL_PLAN:\n");
        sb.append(logicalPlan.pretty());

        sb.append("\nPHYSICAL_PLAN:\n");
        sb.append(physicalPlan.pretty());

        return sb.toString();
    }
}


