package ru.open.cu.student.optimizer.node;

final class PhysicalPlanPrinter {
    private PhysicalPlanPrinter() {
    }

    static void print(StringBuilder sb, PhysicalPlanNode node, int indent) {
        sb.append("  ".repeat(Math.max(0, indent)))
                .append(node.displayName())
                .append('\n');
        for (PhysicalPlanNode child : node.children()) {
            print(sb, child, indent + 1);
        }
    }
}


