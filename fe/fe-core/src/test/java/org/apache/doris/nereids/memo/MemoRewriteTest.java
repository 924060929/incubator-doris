package org.apache.doris.nereids.memo;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Optional;

public class MemoRewriteTest implements PatternMatchSupported {
    private ConnectContext connectContext = MemoTestUtils.createConnectContext();

    @Test
    public void testRewriteBottomPlanToOnePlan() {
        LogicalOlapScan student = new LogicalOlapScan(PlanConstructor.student);
        LogicalOlapScan score = new LogicalOlapScan(PlanConstructor.score);

        PlanChecker.from(connectContext, student)
                .applyBottomUp(
                        logicalOlapScan().when(scan -> Objects.equals(student, scan)).then(scan -> score)
                )
                .checkGroupNum(1)
                .checkFirstRootLogicalPlan(score)
                .matches(logicalOlapScan().when(score::equals));
    }

    @Test
    public void testRewriteBottomPlanToMultiPlan() {
        LogicalOlapScan student = new LogicalOlapScan(PlanConstructor.student);
        LogicalOlapScan score = new LogicalOlapScan(PlanConstructor.score);
        LogicalLimit<LogicalOlapScan> limit = new LogicalLimit<>(1, 0, score);

        PlanChecker.from(connectContext, student)
                .applyBottomUp(
                        logicalOlapScan().when(scan -> Objects.equals(student, scan)).then(scan -> limit)
                )
                .checkGroupNum(2)
                .checkFirstRootLogicalPlan(limit)
                .matches(
                        logicalLimit(
                                any().when(child -> Objects.equals(child, score))
                        ).when(limit::equals)
                );
    }

    @Test
    public void testRewriteUnboundPlanToBound() {
        UnboundRelation unboundTable = new UnboundRelation(ImmutableList.of("score"));
        LogicalOlapScan boundTable = new LogicalOlapScan(PlanConstructor.score);

        PlanChecker.from(connectContext, unboundTable)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertTrue(logicalProperties instanceof UnboundLogicalProperties);
                })
                .applyBottomUp(unboundRelation().then(unboundRelation -> boundTable))
                .checkGroupNum(1)
                .checkFirstRootLogicalPlan(boundTable)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertEquals(
                            boundTable.getLogicalProperties().getOutput(), logicalProperties.getOutput());
                })
                .matches(
                        logicalOlapScan().when(boundTable::equals)
                );
    }

    @Test
    public void testRecomputeLogicalProperties() {
        UnboundRelation unboundTable = new UnboundRelation(ImmutableList.of("score"));
        LogicalLimit<UnboundRelation> unboundLimit = new LogicalLimit<>(1, 0, unboundTable);

        LogicalOlapScan boundTable = new LogicalOlapScan(PlanConstructor.score);
        LogicalLimit<Plan> boundLimit = unboundLimit.withChildren(ImmutableList.of(boundTable));

        PlanChecker.from(connectContext, unboundLimit)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertTrue(logicalProperties instanceof UnboundLogicalProperties);
                })
                .applyBottomUp(unboundRelation().then(unboundRelation -> boundTable))
                .applyBottomUp(
                        logicalPlan()
                                .when(plan -> plan.canResolve() && !(plan instanceof LeafPlan))
                                .then(LogicalPlan::recomputeLogicalProperties)
                )
                .checkGroupNum(2)
                .checkFirstRootLogicalPlan(boundLimit)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertEquals(
                            boundTable.getLogicalProperties().getOutput(), logicalProperties.getOutput());
                })
                .matches(
                        logicalLimit(
                                logicalOlapScan().when(boundTable::equals)
                        ).when(boundLimit::equals)
                );
    }

    @Test
    public void testEliminateRootWithChildGroup() {
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.score);
        LogicalLimit<Plan> limit = new LogicalLimit<>(1, 0, scan);

        PlanChecker.from(connectContext, limit)
                .applyBottomUp(logicalLimit().then(LogicalLimit::child))
                .checkGroupNum(1)
                .checkGroupExpressionNum(1)
                .checkFirstRootLogicalPlan(scan);
    }

    @Test
    public void testEliminateRootWithChildPlan() {
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.score);
        LogicalLimit<Plan> limit = new LogicalLimit<>(1, 0, scan);

        PlanChecker.from(connectContext, limit)
                .applyBottomUp(logicalLimit(any()).then(LogicalLimit::child))
                .checkGroupNum(1)
                .checkGroupExpressionNum(1)
                .checkFirstRootLogicalPlan(scan);
    }

    @Test
    public void test() {
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(new LogicalLimit<>(10, 0,
                        new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                                Optional.of(new EqualTo(new UnboundSlot("sid"), new UnboundSlot("id"))),
                                new LogicalOlapScan(PlanConstructor.score),
                                new LogicalOlapScan(PlanConstructor.student)
                        )
                ))
                .applyTopDown(
                    logicalLimit(logicalJoin()).then(limit -> {
                        LogicalJoin<GroupPlan, GroupPlan> join = limit.child();
                        switch (join.getJoinType()) {
                            case LEFT_OUTER_JOIN:
                                return join.withChildren(limit.withChildren(join.left()), join.right());
                            case RIGHT_OUTER_JOIN:
                                return join.withChildren(join.left(), limit.withChildren(join.right()));
                            case CROSS_JOIN:
                                return join.withChildren(limit.withChildren(join.left()), limit.withChildren(join.right()));
                            case INNER_JOIN:
                                if (!join.getCondition().isPresent()) {
                                    return join.withChildren(
                                            limit.withChildren(join.left()),
                                            limit.withChildren(join.right())
                                    );
                                } else {
                                    return limit;
                                }
                            case LEFT_ANTI_JOIN:
                                // todo: support anti join.
                            default:
                                return limit;
                        }
                    })
                )
                .matches(
                        logicalJoin(
                                logicalLimit(
                                        logicalOlapScan()
                                ),
                                logicalOlapScan()
                        )
                );
    }


    /**
     * Original:
     * Project(name)
     * |---Project(name)
     *     |---UnboundRelation
     *
     * After rewrite:
     * Project(name)
     * |---Project(rewrite)
     *     |---Project(rewrite_inside)
     *         |---UnboundRelation
     */
    @Test
    public void testRewrite() {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject insideProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                unboundRelation
        );
        LogicalProject rootProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                insideProject
        );

        // Project -> Project -> Relation
        Memo memo = new Memo(rootProject);
        Group leafGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 0).findFirst().get();
        Group targetGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 1).findFirst().get();
        LogicalProject rewriteInsideProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("rewrite_inside", StringType.INSTANCE,
                        false, ImmutableList.of("test"))),
                new GroupPlan(leafGroup)
        );
        LogicalProject rewriteProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("rewrite", StringType.INSTANCE,
                        true, ImmutableList.of("test"))),
                rewriteInsideProject
        );
        memo.copyIn(rewriteProject, targetGroup, true);

        Assertions.assertEquals(4, memo.getGroups().size());
        Plan node = memo.copyOut();
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals("name", ((LogicalProject<?>) node).getProjects().get(0).getName());
        node = node.child(0);
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals("rewrite", ((LogicalProject<?>) node).getProjects().get(0).getName());
        node = node.child(0);
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals("rewrite_inside", ((LogicalProject<?>) node).getProjects().get(0).getName());
        node = node.child(0);
        Assertions.assertTrue(node instanceof UnboundRelation);
        Assertions.assertEquals("test", ((UnboundRelation) node).getTableName());
    }

    /**
     * Test rewrite current Plan with its child.
     *
     * Original(Group 2 is root):
     * Group2: Project(outside)
     * Group1: |---Project(inside)
     * Group0:     |---UnboundRelation
     *
     * and we want to rewrite group 2 by Project(inside, GroupPlan(group 0))
     *
     * After rewriting we should get(Group 2 is root):
     * Group2: Project(inside)
     * Group0: |---UnboundRelation
     */
    @Test
    public void testRewriteByChild() {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        LogicalProject<UnboundRelation> insideProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("inside", StringType.INSTANCE, true, ImmutableList.of("test"))),
                unboundRelation
        );
        LogicalProject<LogicalProject<UnboundRelation>> rootProject = new LogicalProject<>(
                ImmutableList.of(new SlotReference("outside", StringType.INSTANCE, true, ImmutableList.of("test"))),
                insideProject
        );

        // Project -> Project -> Relation
        Memo memo = new Memo(rootProject);
        Group leafGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 0).findFirst().get();
        Group targetGroup = memo.getGroups().stream().filter(g -> g.getGroupId().asInt() == 2).findFirst().get();
        LogicalPlan rewriteProject = insideProject.withChildren(Lists.newArrayList(new GroupPlan(leafGroup)));
        memo.copyIn(rewriteProject, targetGroup, true);

        Assertions.assertEquals(2, memo.getGroups().size());
        Plan node = memo.copyOut();
        Assertions.assertTrue(node instanceof LogicalProject);
        Assertions.assertEquals(insideProject.getProjects().get(0), ((LogicalProject<?>) node).getProjects().get(0));
        node = node.child(0);
        Assertions.assertTrue(node instanceof UnboundRelation);
        Assertions.assertEquals("test", ((UnboundRelation) node).getTableName());

        // check Group 1's GroupExpression is not in GroupExpressionMaps anymore
        GroupExpression groupExpression = new GroupExpression(rewriteProject, Lists.newArrayList(leafGroup));
        Assertions.assertEquals(2,
                memo.getGroupExpressions().get(groupExpression).getOwnerGroup().getGroupId().asInt());
    }

}
