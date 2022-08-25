// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.memo;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Representation for memo in cascades optimizer.
 */
public class Memo {
    // generate group id in memo is better for test, since we can reproduce exactly same Memo.
    private final IdGenerator<GroupId> groupIdGenerator = GroupId.createGenerator();
    private final Map<GroupId, Group> groups = Maps.newLinkedHashMap();
    // we could not use Set, because Set does not have get method.
    private final Map<GroupExpression, GroupExpression> groupExpressions = Maps.newHashMap();
    private Group root;

    public Memo(Plan plan) {
        root = init(plan);
    }

    public Group getRoot() {
        return root;
    }

    public List<Group> getGroups() {
        return ImmutableList.copyOf(groups.values());
    }

    public Map<GroupExpression, GroupExpression> getGroupExpressions() {
        return groupExpressions;
    }

    /**
     * Add plan to Memo.
     * TODO: add ut later
     *
     * @param node {@link Plan} or {@link Expression} to be added
     * @param target target group to add node. null to generate new Group
     * @param rewrite whether to rewrite the node to the target group
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    public Pair<Boolean, GroupExpression> copyIn(Plan node, @Nullable Group target, boolean rewrite) {
        if (rewrite) {
            return rewrite(node, target);
        }

        Optional<GroupExpression> groupExpr = node.getGroupExpression();
        if (!rewrite && groupExpr.isPresent() && groupExpressions.containsKey(groupExpr.get())) {
            return new Pair<>(false, groupExpr.get());
        }
        List<Group> childrenGroups = Lists.newArrayList();
        for (int i = 0; i < node.children().size(); i++) {
            Plan child = node.children().get(i);
            if (child instanceof GroupPlan) {
                childrenGroups.add(((GroupPlan) child).getGroup());
            } else if (child.getGroupExpression().isPresent()) {
                childrenGroups.add(child.getGroupExpression().get().getOwnerGroup());
            } else {
                childrenGroups.add(copyIn(child, null, rewrite).second.getOwnerGroup());
            }
        }
        node = replaceChildrenToGroupPlan(node, childrenGroups);
        GroupExpression newGroupExpression = new GroupExpression(node);
        newGroupExpression.setChildren(childrenGroups);
        if (rewrite) {
            return rewriteGroupExpression(newGroupExpression, target, node.getLogicalProperties());
        } else {
            return insertGroupExpression(newGroupExpression, target, node.getLogicalProperties());
        }
        // TODO: need to derive logical property if generate new group. currently we not copy logical plan into
    }

    public Plan copyOut() {
        return copyOut(root);
    }

    /**
     * copyOut the group.
     * @param group the group what want to copyOut
     * @return plan
     */
    public Plan copyOut(Group group) {
        GroupExpression logicalExpression = group.getLogicalExpression();
        List<Plan> childrenNode = Lists.newArrayList();
        for (Group child : logicalExpression.children()) {
            childrenNode.add(copyOut(child));
        }
        Plan result = logicalExpression.getPlan();
        if (result.children().size() == 0) {
            return result;
        }
        return result.withChildren(childrenNode);
    }

    /**
     * Utility function to create a new {@link CascadesContext} with this Memo.
     */
    public CascadesContext newCascadesContext(StatementContext statementContext) {
        return new CascadesContext(this, statementContext);
    }

    /**
     * init memo by a first plan.
     * @param plan first plan
     * @return plan's corresponding group
     */
    private Group init(Plan plan) {
        Preconditions.checkArgument(!(plan instanceof GroupPlan), "Cannot init memo by a GroupPlan");
        Preconditions.checkArgument(!plan.getGroupExpression().isPresent(),
                "Cannot init memo by a plan which contains a groupExpression");

        // initialize children recursively
        List<Group> childrenGroups = plan.children()
                .stream()
                .map(this::init)
                .collect(ImmutableList.toImmutableList());

        plan = replaceChildrenToGroupPlan(plan, childrenGroups);
        GroupExpression newGroupExpression = new GroupExpression(plan, childrenGroups);
        Group group = new Group(groupIdGenerator.getNextId(), newGroupExpression, plan.getLogicalProperties());

        groups.put(group.getGroupId(), group);
        if (groupExpressions.containsKey(newGroupExpression)) {
            throw new IllegalStateException("groupExpression already exists in memo, maybe a bug");
        }
        groupExpressions.put(newGroupExpression, newGroupExpression);
        return group;
    }

    /**
     * add or replace the plan into the target group
     * @param plan the plan which want to rewrite or added
     * @param targetGroup target group to replace plan. null to generate new Group
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    public Pair<Boolean, GroupExpression> rewrite(Plan plan, @Nullable Group targetGroup) {
        Preconditions.checkArgument(plan != null, "plan can not be null");
        Preconditions.checkArgument(plan instanceof LogicalPlan, "only logical plan can be rewrite");

        LogicalProperties logicalProperties = plan.getLogicalProperties();
        if (plan instanceof GroupPlan || plan.getGroupExpression().isPresent()) {
            GroupExpression existedLogicalExpression = plan instanceof GroupPlan
                    ? ((GroupPlan) plan).getGroup().getLogicalExpression()
                    : plan.getGroupExpression().get();
            if (targetGroup != null) {
                Group fromGroup = existedLogicalExpression.getOwnerGroup();

                // clear targetGroup, from exist group move all logical groupExpression
                // and logicalProperties to target group
                rewrite(fromGroup, targetGroup, logicalProperties);
            }
            return Pair.create(false, existedLogicalExpression);
        }

        List<Group> childrenGroups = Lists.newArrayList();
        for (int i = 0; i < plan.children().size(); i++) {
            Plan child = plan.children().get(i);
            if (child instanceof GroupPlan) {
                childrenGroups.add(((GroupPlan) child).getGroup());
            } else if (child.getGroupExpression().isPresent()) {
                childrenGroups.add(child.getGroupExpression().get().getOwnerGroup());
            } else {
                childrenGroups.add(rewrite(child, null).second.getOwnerGroup());
            }
        }

        plan = replaceChildrenToGroupPlan(plan, childrenGroups);
        GroupExpression newGroupExpression = new GroupExpression(plan, childrenGroups);

        GroupExpression existedExpression = groupExpressions.get(newGroupExpression);
        if (existedExpression == null) {
            if (targetGroup == null) {
                // if not exist target group and not exist the same group expression, then create new group
                Group newGroup = new Group(groupIdGenerator.getNextId(), newGroupExpression,
                        logicalProperties);
                groups.put(newGroup.getGroupId(), newGroup);
            } else {
                // if exist the target group, clear all origin group expressions in the
                // existedExpression's owner group and reset logical properties, the
                // newGroupExpression is the init logical group expression
                reInitGroup(targetGroup, newGroupExpression, logicalProperties);
            }
            return Pair.create(true, newGroupExpression);
        } else {
            if (targetGroup != null && !targetGroup.equals(existedExpression.getOwnerGroup())) {
                existedExpression.propagateApplied(newGroupExpression);
                moveParentExpressionsReference(existedExpression.getOwnerGroup(), targetGroup);
                recycleGroup(existedExpression.getOwnerGroup());
                reInitGroup(targetGroup, newGroupExpression, logicalProperties);
                return Pair.create(true, newGroupExpression);
            } else {
                recycleExpression(newGroupExpression);
                return Pair.create(false, existedExpression);
            }
        }
    }

    /**
     * Insert groupExpression to target group.
     * If group expression is already in memo and target group is not null, we merge two groups.
     * If target is null, generate new group.
     * If target is not null, add group expression to target group
     *
     * @param groupExpression groupExpression to insert
     * @param target target group to insert groupExpression
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    private Pair<Boolean, GroupExpression> insertGroupExpression(
            GroupExpression groupExpression, Group target, LogicalProperties logicalProperties) {
        GroupExpression existedGroupExpression = groupExpressions.get(groupExpression);
        if (existedGroupExpression != null) {
            if (target != null && !target.getGroupId().equals(existedGroupExpression.getOwnerGroup().getGroupId())) {
                mergeGroup(existedGroupExpression.getOwnerGroup(), target);
            }
            return new Pair<>(false, existedGroupExpression);
        }
        if (target != null) {
            target.addGroupExpression(groupExpression);
        } else {
            Group group = new Group(groupIdGenerator.getNextId(), groupExpression, logicalProperties);
            groups.put(group.getGroupId(), group);
        }
        groupExpressions.put(groupExpression, groupExpression);
        return new Pair<>(true, groupExpression);
    }

    /**
     * Rewrite groupExpression to target group.
     * If group expression is already in memo, we replace logical properties regardless the target group present or not
     *     for replace UnboundLogicalProperties to LogicalProperties
     * If target is null, generate new group.
     * If target is not null, rewrite the groupExpression to target group.
     *
     * @param groupExpression groupExpression to rewrite old one
     * @param target target group to rewrite groupExpression
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    private Pair<Boolean, GroupExpression> rewriteGroupExpression(
            GroupExpression groupExpression, Group target, LogicalProperties logicalProperties) {
        boolean newGroupExpressionGenerated = true;
        GroupExpression existedGroupExpression = groupExpressions.get(groupExpression);
        /*
         * here we need to handle one situation that original target is not the same with
         * existedGroupExpression.getOwnerGroup(). In this case, if we change target to
         * existedGroupExpression.getOwnerGroup(), we could not rewrite plan as we expected and the plan
         * will not be changed anymore.
         * Think below example:
         * We have a plan like this:
         * Original (Group 2 is root):
         * Group2: Project(outside)
         * Group1: |---Project(inside)
         * Group0:     |---UnboundRelation
         *
         * and we want to rewrite group 2 by Project(inside, GroupPlan(group 0))
         *
         * After rewriting we should get (Group 2 is root):
         * Group2: Project(inside)
         * Group0: |---UnboundRelation
         *
         * Group1: Project(inside)
         *
         * After rewriting, Group 1's GroupExpression is not in GroupExpressionsMap anymore and Group 1 is unreachable.
         * Merge Group 1 into Group 2 is better, but in consideration of there is others way to let a Group take into
         * unreachable. There's no need to complicate to add a merge step. Instead, we need to have a clear step to
         * remove unreachable groups and GroupExpressions after rewrite.
         * TODO: add a clear groups function to memo.
         */
        if (existedGroupExpression != null
                && (target == null || target.equals(existedGroupExpression.getOwnerGroup()))) {
            target = existedGroupExpression.getOwnerGroup();
            newGroupExpressionGenerated = false;
        }
        if (target != null) {
            GroupExpression oldExpression = target.rewriteLogicalExpression(groupExpression, logicalProperties);
            groupExpressions.remove(oldExpression);
        } else {
            Group group = new Group(groupIdGenerator.getNextId(), groupExpression, logicalProperties);
            groups.put(group.getGroupId(), group);
        }
        groupExpressions.put(groupExpression, groupExpression);
        return new Pair<>(newGroupExpressionGenerated, groupExpression);
    }

    /**
     * Merge two groups.
     * 1. find all group expression which has source as child
     * 2. replace its child with destination
     * 3. remove redundant group expression after replace child
     * 4. move all group expression in source to destination
     *
     * @param source source group
     * @param destination destination group
     * @return merged group
     */
    private Group mergeGroup(Group source, Group destination) {
        if (source.equals(destination)) {
            return source;
        }
        List<GroupExpression> needReplaceChild = Lists.newArrayList();
        groupExpressions.values().forEach(groupExpression -> {
            if (groupExpression.children().contains(source)) {
                if (groupExpression.getOwnerGroup().equals(destination)) {
                    // cycle, we should not merge
                    return;
                }
                needReplaceChild.add(groupExpression);
            }
        });
        for (GroupExpression groupExpression : needReplaceChild) {
            groupExpressions.remove(groupExpression);
            List<Group> children = groupExpression.children();
            // TODO: use a better way to replace child, avoid traversing all groupExpression
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i).equals(source)) {
                    children.set(i, destination);
                }
            }
            GroupExpression that = groupExpressions.get(groupExpression);
            if (that != null && that.getOwnerGroup() != null
                    && !that.getOwnerGroup().equals(groupExpression.getOwnerGroup())) {
                // remove groupExpression from its owner group to avoid adding it to that.getOwnerGroup()
                // that.getOwnerGroup() already has this groupExpression.
                Group ownerGroup = groupExpression.getOwnerGroup();
                groupExpression.getOwnerGroup().removeGroupExpression(groupExpression);
                mergeGroup(ownerGroup, that.getOwnerGroup());
            } else {
                groupExpressions.put(groupExpression, groupExpression);
            }
        }
        if (!source.equals(destination)) {
            source.moveLogicalExpressionOwnership(destination);
            source.movePhysicalExpressionOwnership(destination);
            groups.remove(source.getGroupId());
        }
        return destination;
    }

    /**
     * Add enforcer expression into the target group.
     */
    public void addEnforcerPlan(GroupExpression groupExpression, Group group) {
        groupExpression.setOwnerGroup(group);
    }

    private void rewrite(Group fromGroup, Group toGroup, LogicalProperties logicalProperties) {
        List<GroupExpression> logicalExpressions = fromGroup.clearLogicalExpressions();
        recycleGroup(fromGroup);

        recycleLogicalExpressions(toGroup);
        recyclePhysicalExpressions(toGroup);

        for (GroupExpression logicalExpression : logicalExpressions) {
            toGroup.addLogicalExpression(logicalExpression);
        }
        toGroup.setLogicalProperties(logicalProperties);
        if (root == fromGroup) {
            root = toGroup;
        }
    }

    private void reInitGroup(Group group, GroupExpression initLogicalExpression, LogicalProperties logicalProperties) {
        recycleLogicalExpressions(group);
        recyclePhysicalExpressions(group);

        group.setLogicalProperties(logicalProperties);
        group.addLogicalExpression(initLogicalExpression);

        // note: put newGroupExpression must behind recycle existedExpression(reInitGroup method),
        //       because existedExpression maybe equal to the newGroupExpression and recycle
        //       existedExpression will recycle newGroupExpression
        groupExpressions.put(initLogicalExpression, initLogicalExpression);
    }

    private Plan replaceChildrenToGroupPlan(Plan plan, List<Group> childrenGroups) {
        if (childrenGroups.isEmpty()) {
            return plan;
        }
        List<Plan> groupPlanChildren = childrenGroups.stream()
                .map(GroupPlan::new)
                .collect(ImmutableList.toImmutableList());
        LogicalProperties logicalProperties = plan.getLogicalProperties();
        return plan.withChildren(groupPlanChildren)
            .withLogicalProperties(Optional.of(logicalProperties));
    }

    private void moveParentExpressionsReference(Group fromGroup, Group toGroup) {
        for (GroupExpression parentGroupExpression : fromGroup.getParentGroupExpressions()) {
            /*
             * the scenarios that 'parentGroupExpression == toGroup': eliminate the root group.
             * the fromGroup is group 1, the toGroup is group 2, we can not replace group2's
             * groupExpressions reference the child group which is group 2 (reference itself).
             *
             *   A(group 2)            B(group 2)
             *   |                     |
             *   B(group 1)      =>    C(group 0)
             *   |
             *   C(group 0)
             */
            if (parentGroupExpression.getOwnerGroup() != toGroup) {
                parentGroupExpression.replaceChild(fromGroup, toGroup);
            }
        }
    }

    private void recycleGroup(Group group) {
        if (groups.get(group.getGroupId()) == group) {
            groups.remove(group.getGroupId());
        }
        recycleLogicalExpressions(group);
        recyclePhysicalExpressions(group);
    }

    private void recycleLogicalExpressions(Group group) {
        if (!group.getLogicalExpressions().isEmpty()) {
            for (GroupExpression logicalExpression : group.getLogicalExpressions()) {
                recycleExpression(logicalExpression);
            }
            group.clearLogicalExpressions();
        }
    }

    private void recyclePhysicalExpressions(Group group) {
        if (!group.getPhysicalExpressions().isEmpty()) {
            for (GroupExpression physicalExpression : group.getPhysicalExpressions()) {
                recycleExpression(physicalExpression);
            }
            group.clearPhysicalExpressions();
        }
    }

    private void recycleExpression(GroupExpression groupExpression) {
        if (groupExpressions.get(groupExpression) == groupExpression) {
            groupExpressions.remove(groupExpression);
        }
        for (Group childGroup : groupExpression.children()) {
            // if not any groupExpression reference child group, then recycle the child group
            if (childGroup.removeParentExpression(groupExpression) == 0) {
                recycleGroup(childGroup);
            }
        }
    }
}
