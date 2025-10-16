package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectAccessPathResult;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.NestedColumnPrunable;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AccessPathPlanCollector extends DefaultPlanVisitor<Void, StatementContext> {
    private Multimap<Integer, CollectAccessPathResult> allSlotToAccessPaths = LinkedHashMultimap.create();
    private Map<Slot, List<CollectAccessPathResult>> scanSlotToAccessPaths = new LinkedHashMap<>();

    public Map<Slot, List<CollectAccessPathResult>> collect(Plan root, StatementContext context) {
        root.accept(this, context);
        return scanSlotToAccessPaths;
    }

    @Override
    public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, StatementContext context) {
        boolean bottomFilter = filter.child().arity() == 0;
        collectByExpressions(filter, context, bottomFilter);
        return filter.child().accept(this, context);
    }

    @Override
    public Void visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, StatementContext context) {
        AccessPathExpressionCollector exprCollector
                = new AccessPathExpressionCollector(context, allSlotToAccessPaths, false);
        for (Slot slot : cteProducer.getOutput()) {
            if (slot.getDataType() instanceof NestedColumnPrunable) {
                exprCollector.collect(slot);
            }
        }
        cteProducer.child().accept(this, context);
        return null;
    }

    @Override
    public Void visitLogicalUnion(LogicalUnion union, StatementContext context) {
        AccessPathExpressionCollector exprCollector
                = new AccessPathExpressionCollector(context, allSlotToAccessPaths, false);
        List<NamedExpression> unionOutputs = union.getOutputs();
        List<List<SlotReference>> regularChildrenOutputs = union.getRegularChildrenOutputs();
        for (int i = 0; i < unionOutputs.size(); i++) {
            NamedExpression unionOutput = unionOutputs.get(i);
            if (unionOutput.getDataType() instanceof NestedColumnPrunable) {
                for (List<SlotReference> childOutputs : regularChildrenOutputs) {
                    exprCollector.collect(childOutputs.get(i));
                }
            }
        }

        for (Plan child : union.children()) {
            child.accept(this, context);
        }
        return null;
    }

    @Override
    public Void visitLogicalOlapScan(LogicalOlapScan olapScan, StatementContext context) {
        for (Slot slot : olapScan.getOutput()) {
            if (!(slot.getDataType() instanceof NestedColumnPrunable)) {
                continue;
            }
            Collection<CollectAccessPathResult> accessPaths = allSlotToAccessPaths.get(slot.getExprId().asInt());
            if (!accessPaths.isEmpty()) {
                scanSlotToAccessPaths.put(slot, new ArrayList<>(accessPaths));
            }
        }
        return null;
    }

    @Override
    public Void visitLogicalFileScan(LogicalFileScan fileScan, StatementContext context) {
        for (Slot slot : fileScan.getOutput()) {
            if (!(slot.getDataType() instanceof NestedColumnPrunable)) {
                continue;
            }
            Collection<CollectAccessPathResult> accessPaths = allSlotToAccessPaths.get(slot.getExprId().asInt());
            if (!accessPaths.isEmpty()) {
                scanSlotToAccessPaths.put(slot, new ArrayList<>(accessPaths));
            }
        }
        return null;
    }

    @Override
    public Void visit(Plan plan, StatementContext context) {
        collectByExpressions(plan, context);

        for (Plan child : plan.children()) {
            child.accept(this, context);
        }
        return null;
    }

    private void collectByExpressions(Plan plan, StatementContext context) {
        collectByExpressions(plan, context, false);
    }

    private void collectByExpressions(Plan plan, StatementContext context, boolean bottomPredicate) {
        AccessPathExpressionCollector exprCollector
                = new AccessPathExpressionCollector(context, allSlotToAccessPaths, bottomPredicate);
        for (Expression expression : plan.getExpressions()) {
            exprCollector.collect(expression);
        }
    }
}
