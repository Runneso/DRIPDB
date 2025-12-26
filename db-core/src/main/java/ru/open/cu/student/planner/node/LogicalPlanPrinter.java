package ru.open.cu.student.planner.node;

final class LogicalPlanPrinter {
    private LogicalPlanPrinter() {
    }

    static void print(StringBuilder sb, LogicalPlanNode node, int indent) {
        sb.append("  ".repeat(Math.max(0, indent)))
                .append(node.displayName())
                .append('\n');
        for (LogicalPlanNode child : node.children()) {
            print(sb, child, indent + 1);
        }
    }
}


