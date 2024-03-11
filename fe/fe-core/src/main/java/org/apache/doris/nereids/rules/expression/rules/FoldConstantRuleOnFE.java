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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.ExpressionBottomUpRewriter;
import org.apache.doris.nereids.rules.expression.ExpressionListenerMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionMatchingAction;
import org.apache.doris.nereids.rules.expression.ExpressionMatchingContext;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionTraverseListener;
import org.apache.doris.nereids.rules.expression.ExpressionTraverseListenerFactory;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionEvaluator;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentCatalog;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Database;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncryptKeyRef;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Password;
import org.apache.doris.nereids.trees.expressions.functions.scalar.User;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Version;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * evaluate an expression on fe.
 */
public class FoldConstantRuleOnFE implements ExpressionPatternRuleFactory, ExpressionTraverseListenerFactory {

    public static final FoldConstantRuleOnFE INSTANCE;
    private static final ExpressionBottomUpRewriter REWRITER;

    // record whether current expression is in an aggregate function with distinct,
    // if is, we will skip to fold constant
    private static final ListenAggDistinct LISTEN_AGG_DISTINCT;
    private static final CheckWhetherUnderAggDistinct NOT_UNDER_AGG_DISTINCT;

    // NOTE: use this static block to ensure initialize the order of the fields,
    //       we should initialize INSTANCE, then INSTANCE REWRITER
    static {
        INSTANCE = new FoldConstantRuleOnFE();
        LISTEN_AGG_DISTINCT = new ListenAggDistinct();
        NOT_UNDER_AGG_DISTINCT = new CheckWhetherUnderAggDistinct();
        REWRITER = ExpressionRewrite.bottomUp(INSTANCE);
    }

    @Override
    public List<ExpressionListenerMatcher<? extends Expression>> buildListeners() {
        return ImmutableList.of(
                listenerType(AggregateFunction.class)
                        .when(AggregateFunction::isDistinct)
                        .then(LISTEN_AGG_DISTINCT.as()),

                listenerType(AggregateExpression.class)
                        .when(AggregateExpression::isDistinct)
                        .then(LISTEN_AGG_DISTINCT.as())
        );
    }

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matches(EncryptKeyRef.class, FoldConstantRuleOnFE::foldEncryptKeyRef),
                matches(EqualTo.class, FoldConstantRuleOnFE::foldEqualTo),
                matches(GreaterThan.class, FoldConstantRuleOnFE::foldGreaterThan),
                matches(GreaterThanEqual.class, FoldConstantRuleOnFE::foldGreaterThanEqual),
                matches(LessThan.class, FoldConstantRuleOnFE::foldLessThan),
                matches(LessThanEqual.class, FoldConstantRuleOnFE::foldLessThanEqual),
                matches(NullSafeEqual.class, FoldConstantRuleOnFE::foldNullSafeEqual),
                matches(Not.class, FoldConstantRuleOnFE::foldNot),
                matches(Database.class, FoldConstantRuleOnFE::foldDatabase),
                matches(CurrentUser.class, FoldConstantRuleOnFE::foldCurrentUser),
                matches(CurrentCatalog.class, FoldConstantRuleOnFE::foldCurrentCatalog),
                matches(User.class, FoldConstantRuleOnFE::foldUser),
                matches(ConnectionId.class, FoldConstantRuleOnFE::foldConnectionId),
                matches(And.class, FoldConstantRuleOnFE::foldAnd),
                matches(Or.class, FoldConstantRuleOnFE::foldOr),
                matches(Cast.class, FoldConstantRuleOnFE::foldCast),
                matches(BoundFunction.class, FoldConstantRuleOnFE::foldBoundFunction),
                matches(BinaryArithmetic.class, FoldConstantRuleOnFE::foldBinaryArithmetic),
                matches(CaseWhen.class, FoldConstantRuleOnFE::foldCaseWhen),
                matches(If.class, FoldConstantRuleOnFE::foldIf),
                matches(InPredicate.class, FoldConstantRuleOnFE::foldInPredicate),
                matches(IsNull.class, FoldConstantRuleOnFE::foldIsNull),
                matches(TimestampArithmetic.class, FoldConstantRuleOnFE::foldTimestampArithmetic),
                matches(Password.class, FoldConstantRuleOnFE::foldPassword),
                matches(Array.class, FoldConstantRuleOnFE::foldArray),
                matches(Date.class, FoldConstantRuleOnFE::foldDate),
                matches(Version.class, FoldConstantRuleOnFE::foldVersion)
        );
    }

    public static Expression evaluate(Expression expression, ExpressionRewriteContext expressionRewriteContext) {
        return REWRITER.rewrite(expression, expressionRewriteContext);
    }

    private static Expression foldEncryptKeyRef(ExpressionMatchingContext<EncryptKeyRef> context) {
        EncryptKeyRef encryptKeyRef = context.expr;
        String dbName = encryptKeyRef.getDbName();
        ConnectContext connectContext = context.cascadesContext.getConnectContext();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = connectContext.getDatabase();
        }
        if ("".equals(dbName)) {
            throw new AnalysisException("DB " + dbName + "not found");
        }
        org.apache.doris.catalog.Database database =
                Env.getCurrentEnv().getInternalCatalog().getDbNullable(dbName);
        if (database == null) {
            throw new AnalysisException("DB " + dbName + "not found");
        }
        EncryptKey encryptKey = database.getEncryptKey(encryptKeyRef.getEncryptKeyName());
        if (encryptKey == null) {
            throw new AnalysisException("Can not found encryptKey" + encryptKeyRef.getEncryptKeyName());
        }
        return new StringLiteral(encryptKey.getKeyString());
    }

    private static Expression foldEqualTo(EqualTo equalTo) {
        Optional<Expression> checkedExpr = preProcess(equalTo);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) equalTo.left()).compareTo((Literal) equalTo.right()) == 0);
    }

    private static Expression foldGreaterThan(GreaterThan greaterThan) {
        Optional<Expression> checkedExpr = preProcess(greaterThan);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) greaterThan.left()).compareTo((Literal) greaterThan.right()) > 0);
    }

    private static Expression foldGreaterThanEqual(GreaterThanEqual greaterThanEqual) {
        Optional<Expression> checkedExpr = preProcess(greaterThanEqual);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) greaterThanEqual.left())
                .compareTo((Literal) greaterThanEqual.right()) >= 0);
    }

    private static Expression foldLessThan(LessThan lessThan) {
        Optional<Expression> checkedExpr = preProcess(lessThan);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) lessThan.left()).compareTo((Literal) lessThan.right()) < 0);
    }

    private static Expression foldLessThanEqual(LessThanEqual lessThanEqual) {
        Optional<Expression> checkedExpr = preProcess(lessThanEqual);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) lessThanEqual.left()).compareTo((Literal) lessThanEqual.right()) <= 0);
    }

    private static Expression foldNullSafeEqual(NullSafeEqual nullSafeEqual) {
        Optional<Expression> checkedExpr = preProcess(nullSafeEqual);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        Literal l = (Literal) nullSafeEqual.left();
        Literal r = (Literal) nullSafeEqual.right();
        if (l.isNullLiteral() && r.isNullLiteral()) {
            return BooleanLiteral.TRUE;
        } else if (!l.isNullLiteral() && !r.isNullLiteral()) {
            return BooleanLiteral.of(l.compareTo(r) == 0);
        } else {
            return BooleanLiteral.FALSE;
        }
    }

    private static Expression foldNot(Not not) {
        Optional<Expression> checkedExpr = preProcess(not);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(!((BooleanLiteral) not.child()).getValue());
    }

    private static Expression foldDatabase(ExpressionMatchingContext<Database> context) {
        String res = ClusterNamespace.getNameFromFullName(context.cascadesContext.getConnectContext().getDatabase());
        return new VarcharLiteral(res);
    }

    private static Expression foldCurrentUser(ExpressionMatchingContext<CurrentUser> context) {
        String res = context.cascadesContext.getConnectContext().getCurrentUserIdentity().toString();
        return new VarcharLiteral(res);
    }

    private static Expression foldCurrentCatalog(ExpressionMatchingContext<CurrentCatalog> context) {
        String res = context.cascadesContext.getConnectContext().getDefaultCatalog();
        return new VarcharLiteral(res);
    }

    private static Expression foldUser(ExpressionMatchingContext<User> context) {
        String res = context.cascadesContext.getConnectContext().getUserIdentity().toString();
        return new VarcharLiteral(res);
    }

    private static Expression foldConnectionId(ExpressionMatchingContext<ConnectionId> context) {
        return new BigIntLiteral(context.cascadesContext.getConnectContext().getConnectionId());
    }

    private static Expression foldAnd(And and) {
        List<Expression> nonTrueLiteral = Lists.newArrayList();
        int nullCount = 0;
        for (Expression e : and.children()) {
            if (BooleanLiteral.FALSE.equals(e)) {
                return BooleanLiteral.FALSE;
            } else if (e instanceof NullLiteral) {
                nullCount++;
                nonTrueLiteral.add(e);
            } else if (!BooleanLiteral.TRUE.equals(e)) {
                nonTrueLiteral.add(e);
            }
        }

        if (nullCount == 0) {
            switch (nonTrueLiteral.size()) {
                case 0:
                    // true and true
                    return BooleanLiteral.TRUE;
                case 1:
                    // true and x
                    return nonTrueLiteral.get(0);
                default:
                    // x and y
                    return and.withChildren(nonTrueLiteral);
            }
        } else if (nullCount == 1) {
            if (nonTrueLiteral.size() == 1) {
                // null and true
                return new NullLiteral(BooleanType.INSTANCE);
            }
            // null and x
            return and.withChildren(nonTrueLiteral);
        } else {
            // null and null
            return new NullLiteral(BooleanType.INSTANCE);
        }
    }

    private static Expression foldOr(Or or) {
        List<Expression> nonFalseLiteral = Lists.newArrayList();
        int nullCount = 0;
        for (Expression e : or.children()) {
            if (BooleanLiteral.TRUE.equals(e)) {
                return BooleanLiteral.TRUE;
            } else if (e instanceof NullLiteral) {
                nullCount++;
                nonFalseLiteral.add(e);
            } else if (!BooleanLiteral.FALSE.equals(e)) {
                nonFalseLiteral.add(e);
            }
        }

        if (nullCount == 0) {
            switch (nonFalseLiteral.size()) {
                case 0:
                    // false or false
                    return BooleanLiteral.FALSE;
                case 1:
                    // false or x
                    return nonFalseLiteral.get(0);
                default:
                    // x or y
                    return or.withChildren(nonFalseLiteral);
            }
        } else if (nullCount == 1) {
            if (nonFalseLiteral.size() == 1) {
                // null or false
                return new NullLiteral(BooleanType.INSTANCE);
            }
            // null or x
            return or.withChildren(nonFalseLiteral);
        } else {
            // null or null
            return new NullLiteral(BooleanType.INSTANCE);
        }
    }

    private static Expression foldCast(Cast cast) {
        Optional<Expression> checkedExpr = preProcess(cast);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        Expression child = cast.child();
        DataType dataType = cast.getDataType();
        // todo: process other null case
        if (child.isNullLiteral()) {
            return new NullLiteral(dataType);
        } else if (child instanceof StringLikeLiteral && dataType instanceof DateLikeType) {
            try {
                return ((DateLikeType) dataType).fromString(((StringLikeLiteral) child).getStringValue());
            } catch (AnalysisException t) {
                if (cast.isExplicitType()) {
                    return new NullLiteral(dataType);
                } else {
                    // If cast is from type coercion, we don't use NULL literal and will throw exception.
                    throw t;
                }
            }
        }
        try {
            return child.checkedCastTo(dataType);
        } catch (Throwable t) {
            return cast;
        }
    }

    private static Expression foldBoundFunction(BoundFunction boundFunction) {
        if (!boundFunction.foldable()) {
            return boundFunction;
        }
        Optional<Expression> checkedExpr = preProcess(boundFunction);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return ExpressionEvaluator.INSTANCE.eval(boundFunction);
    }

    private static Expression foldBinaryArithmetic(BinaryArithmetic binaryArithmetic) {
        Optional<Expression> checkedExpr = preProcess(binaryArithmetic);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return ExpressionEvaluator.INSTANCE.eval(binaryArithmetic);
    }

    private static Expression foldCaseWhen(CaseWhen caseWhen) {
        Expression newDefault = null;
        boolean foundNewDefault = false;

        List<WhenClause> whenClauses = new ArrayList<>();
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            Expression whenOperand = whenClause.getOperand();

            if (!(whenOperand.isLiteral())) {
                whenClauses.add(new WhenClause(whenOperand, whenClause.getResult()));
            } else if (BooleanLiteral.TRUE.equals(whenOperand)) {
                foundNewDefault = true;
                newDefault = whenClause.getResult();
                break;
            }
        }

        Expression defaultResult = caseWhen.getDefaultValue().isPresent()
                ? caseWhen.getDefaultValue().get()
                : null;
        if (foundNewDefault) {
            defaultResult = newDefault;
        }
        if (whenClauses.isEmpty()) {
            return defaultResult == null ? new NullLiteral(caseWhen.getDataType()) : defaultResult;
        }
        if (defaultResult == null) {
            if (caseWhen.getDataType().isNullType()) {
                // if caseWhen's type is NULL_TYPE, means all possible return values are nulls
                // it's safe to return null literal here
                return new NullLiteral();
            } else {
                return new CaseWhen(whenClauses);
            }
        }
        return new CaseWhen(whenClauses, defaultResult);
    }

    private static Expression foldIf(If ifExpr) {
        if (ifExpr.child(0) instanceof NullLiteral || ifExpr.child(0).equals(BooleanLiteral.FALSE)) {
            return ifExpr.child(2);
        } else if (ifExpr.child(0).equals(BooleanLiteral.TRUE)) {
            return ifExpr.child(1);
        }
        return ifExpr;
    }

    private static Expression foldInPredicate(InPredicate inPredicate) {
        Optional<Expression> checkedExpr = preProcess(inPredicate);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        // now the inPredicate contains literal only.
        Expression value = inPredicate.child(0);
        if (value.isNullLiteral()) {
            return new NullLiteral(BooleanType.INSTANCE);
        }
        boolean isOptionContainsNull = false;
        for (Expression item : inPredicate.getOptions()) {
            if (value.equals(item)) {
                return BooleanLiteral.TRUE;
            } else if (item.isNullLiteral()) {
                isOptionContainsNull = true;
            }
        }
        return isOptionContainsNull
                ? new NullLiteral(BooleanType.INSTANCE)
                : BooleanLiteral.FALSE;
    }

    private static Expression foldIsNull(IsNull isNull) {
        Optional<Expression> checkedExpr = preProcess(isNull);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return Literal.of(isNull.child().nullable());
    }

    private static Expression foldTimestampArithmetic(TimestampArithmetic arithmetic) {
        Optional<Expression> checkedExpr = preProcess(arithmetic);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return ExpressionEvaluator.INSTANCE.eval(arithmetic);
    }

    private static Expression foldPassword(Password password) {
        Preconditions.checkArgument(password.child(0) instanceof StringLikeLiteral,
                "argument of password must be string literal");
        String s = ((StringLikeLiteral) password.child()).value;
        return new StringLiteral("*" + DigestUtils.sha1Hex(
                DigestUtils.sha1(s.getBytes())).toUpperCase());
    }

    private static Expression foldArray(Array array) {
        Optional<Expression> checkedExpr = preProcess(array);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        List<Literal> arguments = (List) array.getArguments();
        // we should pass dataType to constructor because arguments maybe empty
        return new ArrayLiteral(arguments, array.getDataType());
    }

    private static Expression foldDate(Date date) {
        Optional<Expression> checkedExpr = preProcess(date);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        Literal child = (Literal) date.child();
        if (child instanceof NullLiteral) {
            return new NullLiteral(date.getDataType());
        }
        DataType dataType = child.getDataType();
        if (dataType.isDateTimeType()) {
            DateTimeLiteral dateTimeLiteral = (DateTimeLiteral) child;
            return new DateLiteral(dateTimeLiteral.getYear(), dateTimeLiteral.getMonth(), dateTimeLiteral.getDay());
        } else if (dataType.isDateTimeV2Type()) {
            DateTimeV2Literal dateTimeLiteral = (DateTimeV2Literal) child;
            return new DateV2Literal(dateTimeLiteral.getYear(), dateTimeLiteral.getMonth(), dateTimeLiteral.getDay());
        }
        return date;
    }

    private static Expression foldVersion(Version version) {
        return new StringLiteral(GlobalVariable.version);
    }

    private static Optional<Expression> preProcess(Expression expression) {
        if (expression instanceof AggregateFunction || expression instanceof TableGeneratingFunction) {
            return Optional.of(expression);
        }
        if (expression instanceof PropagateNullable && ExpressionUtils.hasNullLiteral(expression.getArguments())) {
            return Optional.of(new NullLiteral(expression.getDataType()));
        }
        if (!ExpressionUtils.isAllLiteral(expression.getArguments())) {
            return Optional.of(expression);
        }
        return Optional.empty();
    }

    private static class ListenAggDistinct implements ExpressionTraverseListener<Expression> {
        @Override
        public void onEnter(ExpressionMatchingContext<Expression> context) {
            context.cascadesContext.incrementDistinctAggLevel();
        }

        @Override
        public void onExit(ExpressionMatchingContext<Expression> context, Expression rewritten) {
            context.cascadesContext.decrementDistinctAggLevel();
        }
    }

    private static class CheckWhetherUnderAggDistinct implements Predicate<ExpressionMatchingContext<Expression>> {
        @Override
        public boolean test(ExpressionMatchingContext<Expression> context) {
            return context.cascadesContext.getDistinctAggLevel() == 0;
        }

        public <E extends Expression> Predicate<ExpressionMatchingContext<E>> as() {
            return (Predicate) this;
        }
    }

    private <E extends Expression> ExpressionPatternMatcher<? extends Expression> matches(
            Class<E> clazz, ExpressionMatchingAction<E> action) {
        return matchesType(clazz)
                .whenCtx(NOT_UNDER_AGG_DISTINCT.as())
                .thenApply(action);
    }

    private <E extends Expression> ExpressionPatternMatcher<? extends Expression> matches(
            Class<E> clazz, Function<E, Expression> action) {
        return matchesType(clazz)
                .whenCtx(NOT_UNDER_AGG_DISTINCT.as())
                .then(action);
    }
}
