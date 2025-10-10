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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsKey;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapKeys;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapValues;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StructElement;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NestedColumnPrunable;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.ColumnAccessPaths;
import org.apache.doris.thrift.ColumnNameAccessPath;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;

/** NestedColumnCollector */
public class NestedColumnCollector implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        StatementContext statementContext = jobContext.getCascadesContext().getStatementContext();
        // if (!statementContext.hasNestedColumns()) {
        //     return plan;
        // }

        AccessPathCollector collector = new AccessPathCollector();
        List<Pair<SlotReference, List<String>>> slotToAccessPaths = collector.collectInPlan(plan, statementContext);
        Map<Integer, Pair<DataType, ColumnAccessPaths>> slotToResult
                = pruneDataTypeAndMergeAccessPaths(slotToAccessPaths);
        for (Entry<Integer, Pair<DataType, ColumnAccessPaths>> kv : slotToResult.entrySet()) {
            Integer slotId = kv.getKey();
            DataType prunedDataType = kv.getValue().first;
            ColumnAccessPaths mergedAccessPaths = kv.getValue().second;
            statementContext.setSlotIdToPrunedTypeAndAccessPaths(slotId, prunedDataType, mergedAccessPaths);
        }
        return plan;
    }

    private static Map<Integer, Pair<DataType, ColumnAccessPaths>> pruneDataTypeAndMergeAccessPaths(
            List<Pair<SlotReference, List<String>>> slotToAccessPaths) {
        Map<Integer, Pair<DataType, ColumnAccessPaths>> result = new LinkedHashMap<>();
        Map<SlotReference, DataTypeAccessTree> slotIdToDataTypeTree = new LinkedHashMap<>();

        // first: merge and set access
        for (Pair<SlotReference, List<String>> kv : slotToAccessPaths) {
            SlotReference slot = kv.first;
            List<String> path = kv.second;

            DataTypeAccessTree accessTree = slotIdToDataTypeTree.computeIfAbsent(
                    slot, i -> DataTypeAccessTree.of(slot.getDataType())
            );
            accessTree.setAccessByPath(path, 0);
        }

        // second: use the merged access tree to prune type
        for (Entry<SlotReference, DataTypeAccessTree> kv : slotIdToDataTypeTree.entrySet()) {
            SlotReference slot = kv.getKey();
            DataTypeAccessTree accessTree = kv.getValue();
            DataType prunedDataType = accessTree.pruneDataType().orElse(slot.getDataType());
            ColumnAccessPaths mergedAccessPaths = toAccessPaths(accessTree);
            result.put(slot.getExprId().asInt(), Pair.of(prunedDataType, mergedAccessPaths));
        }
        return result;
    }

    public static ColumnAccessPaths toAccessPaths(DataTypeAccessTree accessTree) {
        ColumnAccessPaths columnAccessPaths = new ColumnAccessPaths();
        List<String> currentPath = new ArrayList<>();


        return columnAccessPaths;
    }

    public static void toAccessPath(
            DataTypeAccessTree accessTree, List<String> currentPath, List<ColumnNameAccessPath> accessPaths) {
        if (accessTree.isAccess()) {

        }
    }

    private class AccessPathCollector extends DefaultExpressionVisitor<Void, CollectorContext> {
        private List<Pair<SlotReference, List<String>>> slotToAccessPaths = new ArrayList<>();

        public List<Pair<SlotReference, List<String>>> collectInPlan(Plan plan, StatementContext statementContext) {
            for (Expression expression : plan.getExpressions()) {
                expression.accept(this, new CollectorContext(statementContext));
            }
            for (Plan child : plan.children()) {
                collectInPlan(child, statementContext);
            }
            return slotToAccessPaths;
        }

        private Void continueCollectAccessPath(Expression expr, CollectorContext context) {
            return expr.accept(this, context);
        }

        @Override
        public Void visit(Expression expr, CollectorContext context) {
            for (Expression child : expr.children()) {
                child.accept(this, new CollectorContext(context.statementContext));
            }
            return null;
        }

        @Override
        public Void visitSlotReference(SlotReference slotReference, CollectorContext context) {
            DataType dataType = slotReference.getDataType();
            if (dataType instanceof NestedColumnPrunable) {
                slotToAccessPaths.add(Pair.of(
                        slotReference,
                        Utils.fastToImmutableList(context.accessPathBuilder.accessPath)
                ));
            }
            return null;
        }

        @Override
        public Void visitAlias(Alias alias, CollectorContext context) {
            return alias.child(0).accept(this, context);
        }

        @Override
        public Void visitCast(Cast cast, CollectorContext context) {
            return cast.child(0).accept(this, context);
        }

        // array element at
        @Override
        public Void visitElementAt(ElementAt elementAt, CollectorContext context) {
            List<Expression> arguments = elementAt.getArguments();
            Expression first = arguments.get(0);
            if (first.getDataType().isArrayType() || first.getDataType().isMapType()
                    || first.getDataType().isVariantType()) {
                context.accessPathBuilder.addPrefix("*");
                continueCollectAccessPath(first, context);

                for (int i = 1; i < arguments.size(); i++) {
                    visit(arguments.get(i), context);
                }
                return null;
            } else {
                return visit(elementAt, context);
            }
        }

        // struct element_at
        @Override
        public Void visitStructElement(StructElement structElement, CollectorContext context) {
            List<Expression> arguments = structElement.getArguments();
            Expression struct = arguments.get(0);
            Expression fieldName = arguments.get(1);
            DataType fieldType = fieldName.getDataType();

            if (fieldName.isLiteral() && (fieldType.isIntegerLikeType() || fieldType.isStringLikeType())) {
                context.accessPathBuilder.addPrefix(((Literal) fieldName).getStringValue());
                return continueCollectAccessPath(struct, context);
            }

            for (Expression argument : arguments) {
                visit(argument, context);
            }
            return null;
        }

        @Override
        public Void visitMapKeys(MapKeys mapKeys, CollectorContext context) {
            context.accessPathBuilder.addPrefix("KEYS");
            return continueCollectAccessPath(mapKeys.getArgument(0), context);
        }

        @Override
        public Void visitMapValues(MapValues mapValues, CollectorContext context) {
            context.accessPathBuilder.addPrefix("VALUES");
            return continueCollectAccessPath(mapValues.getArgument(0), context);
        }

        @Override
        public Void visitMapContainsKey(MapContainsKey mapContainsKey, CollectorContext context) {
            context.accessPathBuilder.addPrefix("KEYS");
            return continueCollectAccessPath(mapContainsKey.getArgument(0), context);
        }

        @Override
        public Void visitMapContainsValue(MapContainsValue mapContainsValue, CollectorContext context) {
            context.accessPathBuilder.addPrefix("VALUES");
            return continueCollectAccessPath(mapContainsValue.getArgument(0), context);
        }

        @Override
        public Void visitArrayMap(ArrayMap arrayMap, CollectorContext context) {
            Lambda lambda = (Lambda) arrayMap.getArgument(0);
            Expression array = arrayMap.getArgument(1);

            String arrayName = lambda.getLambdaArgumentName(0);
            return super.visitArrayMap(arrayMap, context);
        }
    }

    private static class CollectorContext {
        private StatementContext statementContext;
        private AccessPathBuilder accessPathBuilder;

        public CollectorContext(StatementContext statementContext) {
            this.statementContext = statementContext;
            this.accessPathBuilder = new AccessPathBuilder();
        }
    }

    private static class AccessPathBuilder {
        private LinkedList<String> accessPath = new LinkedList<>();

        public AccessPathBuilder addPrefix(String prefix) {
            accessPath.addFirst(prefix);
            return this;
        }
    }

    private static class DataTypeAccessTree {
        private DataType type;
        private boolean access;
        private Map<String, DataTypeAccessTree> children = new LinkedHashMap<>();

        public DataTypeAccessTree(DataType type) {
            this.type = type;
        }

        public void setAccessByPath(List<String> path, int accessIndex) {
            if (accessIndex >= path.size()) {
                return;
            }
            setAccess();

            if (type.isStructType()) {
                String fieldName = path.get(accessIndex);
                DataTypeAccessTree child = children.get(fieldName);
                child.setAccessByPath(path, accessIndex + 1);
                child.setAccess();
                return;
            } else if (type.isArrayType()) {
                DataTypeAccessTree child = children.get("*");
                if (path.get(accessIndex).equals("*")) {
                    // enter this array and skip next *
                    child.setAccessByPath(path, accessIndex + 1);
                }
                // access array always need access the items
                child.setAccess();
                return;
            } else if (type.isMapType()) {
                String fieldName = path.get(accessIndex);
                if (fieldName.equals("*")) {
                    // access value by the key, so we should access key and access value, then prune the value's type.
                    // e.g. map_column['id'] should access the keys, and access the values
                    DataTypeAccessTree keysChild = children.get("KEYS");
                    DataTypeAccessTree valuesChild = children.get("VALUES");
                    keysChild.setAccess();
                    valuesChild.setAccessByPath(path, accessIndex + 1);
                    valuesChild.setAccess();
                    return;
                } else if (fieldName.equals("KEYS")) {
                    // only access the keys and not need enter keys, because it must be primitive type.
                    // e.g. map_keys(map_column)
                    DataTypeAccessTree keysChild = children.get("KEYS");
                    keysChild.setAccess();
                    return;
                } else if (fieldName.equals("VALUES")) {
                    // only access the values without keys, and maybe prune the value's data type.
                    // e.g. map_values(map_columns)[0] will access the array of values first,
                    //      and then access the array, so the access path is ['VALUES', '*']
                    DataTypeAccessTree valuesChild = children.get("VALUES");
                    if (path.size() > accessIndex + 1 && path.get(accessIndex + 1).equals("*")) {
                        // enter this values array and skip next *
                        valuesChild.setAccessByPath(path, accessIndex + 2);
                    }
                    valuesChild.setAccess();
                    return;
                }
            }
            throw new AnalysisException("unsupported data type: " + type);
        }

        public static DataTypeAccessTree of(DataType type) {

            DataTypeAccessTree root = new DataTypeAccessTree(type);
            if (type instanceof StructType) {
                StructType structType = (StructType) type;
                for (Entry<String, StructField> kv : structType.getNameToFields().entrySet()) {
                    root.children.put(kv.getKey(), of(kv.getValue().getDataType()));
                }
            } else if (type instanceof ArrayType) {
                root.children.put("*", of(((ArrayType) type).getItemType()));
            } else if (type instanceof MapType) {
                root.children.put("KEYS", of(((MapType) type).getKeyType()));
                root.children.put("VALUES", of(((MapType) type).getValueType()));
            }
            return root;
        }

        public boolean isAccess() {
            return access;
        }

        private void setAccess() {
            this.access = true;
        }

        public Optional<DataType> pruneDataType() {
            if (!access) {
                return Optional.empty();
            }
            List<Pair<String, DataType>> accessedChildren = new ArrayList<>();

            if (type instanceof StructType) {
                for (Entry<String, DataTypeAccessTree> kv : children.entrySet()) {
                    DataTypeAccessTree childTypeTree = kv.getValue();
                    Optional<DataType> childDataType = childTypeTree.pruneDataType();
                    if (childDataType.isPresent()) {
                        accessedChildren.add(Pair.of(kv.getKey(), childDataType.get()));
                    }
                }
            } else if (type instanceof ArrayType) {
                Optional<DataType> childDataType = children.get("*").pruneDataType();
                if (childDataType.isPresent()) {
                    accessedChildren.add(Pair.of("*", childDataType.get()));
                }
            } else if (type instanceof MapType) {
                DataType prunedValueType = children.get("VALUES").pruneDataType().orElse(((MapType) type).getValueType());
                // can not prune keys but can prune values
                accessedChildren.add(Pair.of("KEYS", ((MapType) type).getKeyType()));
                accessedChildren.add(Pair.of("VALUES", prunedValueType));
            }
            if (accessedChildren.isEmpty()) {
                return Optional.of(type);
            }

            return Optional.of(pruneDataType(type, accessedChildren));
        }

        private DataType pruneDataType(DataType dataType, List<Pair<String, DataType>> newChildrenTypes) {
            if (dataType instanceof StructType) {
                // prune struct fields
                StructType structType = (StructType) dataType;
                Map<String, StructField> nameToFields = structType.getNameToFields();
                List<StructField> newFields = new ArrayList<>();
                for (Pair<String, DataType> kv : newChildrenTypes) {
                    String fieldName = kv.first;
                    StructField originField = nameToFields.get(fieldName);
                    DataType prunedType = kv.second;
                    newFields.add(new StructField(
                            originField.getName(), prunedType, originField.isNullable(), originField.getComment()
                    ));
                }
                return new StructType(newFields);
            } else if (dataType instanceof ArrayType) {
                return ArrayType.of(newChildrenTypes.get(0).second, ((ArrayType) dataType).containsNull());
            } else if (dataType instanceof MapType) {
                return MapType.of(newChildrenTypes.get(0).second, newChildrenTypes.get(1).second);
            } else {
                throw new AnalysisException("unsupported data type: " + dataType);
            }
        }
    }
}
