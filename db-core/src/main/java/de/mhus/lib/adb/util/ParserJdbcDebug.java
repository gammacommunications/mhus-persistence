/**
 * Copyright (C) 2020 Mike Hummel (mh@mhus.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.lib.adb.util;

import de.mhus.lib.adb.ParserJdbc;
import de.mhus.lib.core.MString;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.FullTextSearch;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Block;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.DeclareStatement;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.values.ValuesStatement;

public class ParserJdbcDebug extends ParserJdbc {

    private int level;

    protected void blog(Object object) {
        System.out.print(MString.rep(' ', level));
        System.out.print("--> ");
        if (object == null) System.out.println("?");
        else System.out.println(object.getClass().getSimpleName() + ": " + object);
        level++;
    }

    protected void elog(Object object) {
        level--;
        System.out.print(MString.rep(' ', level));
        System.out.print("<-- ");
        if (object == null) System.out.println("?");
        else System.out.println(object.getClass().getSimpleName() + ": " + object);
    }

    @Override
    public void visit(Select select) {
        blog(select);
        super.visit(select);
        elog(select);
    }

    @Override
    public void visit(WithItem withItem) {
        blog(withItem);
        super.visit(withItem);
        elog(withItem);
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        blog(plainSelect);
        super.visit(plainSelect);
        elog(plainSelect);
    }

    @Override
    public void visit(Table tableName) {
        blog(tableName);
        super.visit(tableName);
        elog(tableName);
    }

    @Override
    public void visit(SubSelect subSelect) {
        blog(subSelect);
        super.visit(subSelect);
        elog(subSelect);
    }

    @Override
    public void visit(Addition addition) {
        blog(addition);
        super.visit(addition);
        elog(addition);
    }

    @Override
    public void visit(AndExpression andExpression) {
        blog(andExpression);
        super.visit(andExpression);
        elog(andExpression);
    }

    @Override
    public void visit(Between between) {
        blog(between);
        super.visit(between);
        elog(between);
    }

    @Override
    public void visit(Column tableColumn) {
        blog(tableColumn);
        super.visit(tableColumn);
        elog(tableColumn);
    }

    @Override
    public void visit(Division division) {
        blog(division);
        super.visit(division);
        elog(division);
    }

    @Override
    public void visit(IntegerDivision division) {
        blog(division);
        super.visit(division);
        elog(division);
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        blog(doubleValue);
        super.visit(doubleValue);
        elog(doubleValue);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        blog(equalsTo);
        super.visit(equalsTo);
        elog(equalsTo);
    }

    @Override
    public void visit(Function function) {
        blog(function);
        super.visit(function);
        elog(function);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        blog(greaterThan);
        super.visit(greaterThan);
        elog(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        blog(greaterThanEquals);
        super.visit(greaterThanEquals);
        elog(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        blog(inExpression);
        super.visit(inExpression);
        elog(inExpression);
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        blog(fullTextSearch);
        super.visit(fullTextSearch);
        elog(fullTextSearch);
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        blog(signedExpression);
        super.visit(signedExpression);
        elog(signedExpression);
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        blog(isNullExpression);
        super.visit(isNullExpression);
        elog(isNullExpression);
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        blog(isBooleanExpression);
        super.visit(isBooleanExpression);
        elog(isBooleanExpression);
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        blog(jdbcParameter);
        super.visit(jdbcParameter);
        elog(jdbcParameter);
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        blog(likeExpression);
        super.visit(likeExpression);
        elog(likeExpression);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        blog(existsExpression);
        super.visit(existsExpression);
        elog(existsExpression);
    }

    @Override
    public void visit(LongValue longValue) {
        blog(longValue);
        super.visit(longValue);
        elog(longValue);
    }

    @Override
    public void visit(MinorThan minorThan) {
        blog(minorThan);
        super.visit(minorThan);
        elog(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        blog(minorThanEquals);
        super.visit(minorThanEquals);
        elog(minorThanEquals);
    }

    @Override
    public void visit(Multiplication multiplication) {
        blog(multiplication);
        super.visit(multiplication);
        elog(multiplication);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        blog(notEqualsTo);
        super.visit(notEqualsTo);
        elog(notEqualsTo);
    }

    @Override
    public void visit(NullValue nullValue) {
        blog(nullValue);
        super.visit(nullValue);
        elog(nullValue);
    }

    @Override
    public void visit(OrExpression orExpression) {
        blog(orExpression);
        super.visit(orExpression);
        elog(orExpression);
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        blog(parenthesis);
        super.visit(parenthesis);
        elog(parenthesis);
    }

    @Override
    public void visit(StringValue stringValue) {
        blog(stringValue);
        super.visit(stringValue);
        elog(stringValue);
    }

    @Override
    public void visit(Subtraction subtraction) {
        blog(subtraction);
        super.visit(subtraction);
        elog(subtraction);
    }

    @Override
    public void visit(NotExpression notExpr) {
        blog(notExpr);
        super.visit(notExpr);
        elog(notExpr);
    }

    @Override
    public void visit(BitwiseRightShift expr) {
        blog(expr);
        super.visit(expr);
        elog(expr);
    }

    @Override
    public void visit(BitwiseLeftShift expr) {
        blog(expr);
        super.visit(expr);
        elog(expr);
    }

    @Override
    public void visit(ExpressionList expressionList) {
        blog(expressionList);
        super.visit(expressionList);
        elog(expressionList);
    }

    @Override
    public void visit(NamedExpressionList namedExpressionList) {
        blog(namedExpressionList);
        super.visit(namedExpressionList);
        elog(namedExpressionList);
    }

    @Override
    public void visit(DateValue dateValue) {
        blog(dateValue);
        super.visit(dateValue);
        elog(dateValue);
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        blog(timestampValue);
        super.visit(timestampValue);
        elog(timestampValue);
    }

    @Override
    public void visit(TimeValue timeValue) {
        blog(timeValue);
        super.visit(timeValue);
        elog(timeValue);
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        blog(caseExpression);
        super.visit(caseExpression);
        elog(caseExpression);
    }

    @Override
    public void visit(WhenClause whenClause) {
        blog(whenClause);
        super.visit(whenClause);
        elog(whenClause);
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        blog(allComparisonExpression);
        super.visit(allComparisonExpression);
        elog(allComparisonExpression);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        blog(anyComparisonExpression);
        super.visit(anyComparisonExpression);
        elog(anyComparisonExpression);
    }

    @Override
    public void visit(SubJoin subjoin) {
        blog(subjoin);
        super.visit(subjoin);
        elog(subjoin);
    }

    @Override
    public void visit(Concat concat) {
        blog(concat);
        super.visit(concat);
        elog(concat);
    }

    @Override
    public void visit(Matches matches) {
        blog(matches);
        super.visit(matches);
        elog(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        blog(bitwiseAnd);
        super.visit(bitwiseAnd);
        elog(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        blog(bitwiseOr);
        super.visit(bitwiseOr);
        elog(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        blog(bitwiseXor);
        super.visit(bitwiseXor);
        elog(bitwiseXor);
    }

    @Override
    public void visit(CastExpression cast) {
        blog(cast);
        super.visit(cast);
        elog(cast);
    }

    @Override
    public void visit(Modulo modulo) {
        blog(modulo);
        super.visit(modulo);
        elog(modulo);
    }

    @Override
    public void visit(AnalyticExpression analytic) {
        blog(analytic);
        super.visit(analytic);
        elog(analytic);
    }

    @Override
    public void visit(SetOperationList list) {
        blog(list);
        super.visit(list);
        elog(list);
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        blog(eexpr);
        super.visit(eexpr);
        elog(eexpr);
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        blog(lateralSubSelect);
        super.visit(lateralSubSelect);
        elog(lateralSubSelect);
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        blog(multiExprList);
        super.visit(multiExprList);
        elog(multiExprList);
    }

    @Override
    public void visit(ValuesList valuesList) {
        blog(valuesList);
        super.visit(valuesList);
        elog(valuesList);
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        blog(iexpr);
        super.visit(iexpr);
        elog(iexpr);
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        blog(jdbcNamedParameter);
        super.visit(jdbcNamedParameter);
        elog(jdbcNamedParameter);
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        blog(oexpr);
        super.visit(oexpr);
        elog(oexpr);
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        blog(rexpr);
        super.visit(rexpr);
        elog(rexpr);
    }

    @Override
    public void visit(RegExpMySQLOperator rexpr) {
        blog(rexpr);
        super.visit(rexpr);
        elog(rexpr);
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        blog(jsonExpr);
        super.visit(jsonExpr);
        elog(jsonExpr);
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        blog(jsonExpr);
        super.visit(jsonExpr);
        elog(jsonExpr);
    }

    @Override
    public void visit(AllColumns allColumns) {
        blog(allColumns);
        super.visit(allColumns);
        elog(allColumns);
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        blog(allTableColumns);
        super.visit(allTableColumns);
        elog(allTableColumns);
    }

    @Override
    public void visit(SelectExpressionItem item) {
        blog(item);
        super.visit(item);
        elog(item);
    }

    @Override
    public void visit(UserVariable var) {
        blog(var);
        super.visit(var);
        elog(var);
    }

    @Override
    public void visit(NumericBind bind) {
        blog(bind);
        super.visit(bind);
        elog(bind);
    }

    @Override
    public void visit(KeepExpression aexpr) {
        blog(aexpr);
        super.visit(aexpr);
        elog(aexpr);
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        blog(groupConcat);
        super.visit(groupConcat);
        elog(groupConcat);
    }

    @Override
    public void visit(ValueListExpression valueList) {
        blog(valueList);
        super.visit(valueList);
        elog(valueList);
    }

    @Override
    public void visit(Delete delete) {
        blog(delete);
        super.visit(delete);
        elog(delete);
    }

    @Override
    public void visit(Update update) {
        blog(update);
        super.visit(update);
        elog(update);
    }

    @Override
    public void visit(Insert insert) {
        blog(insert);
        super.visit(insert);
        elog(insert);
    }

    @Override
    public void visit(Replace replace) {
        blog(replace);
        super.visit(replace);
        elog(replace);
    }

    @Override
    public void visit(Drop drop) {
        blog(drop);
        super.visit(drop);
        elog(drop);
    }

    @Override
    public void visit(Truncate truncate) {
        blog(truncate);
        super.visit(truncate);
        elog(truncate);
    }

    @Override
    public void visit(CreateIndex createIndex) {
        blog(createIndex);
        super.visit(createIndex);
        elog(createIndex);
    }

    @Override
    public void visit(CreateTable create) {
        blog(create);
        super.visit(create);
        elog(create);
    }

    @Override
    public void visit(CreateView createView) {
        blog(createView);
        super.visit(createView);
        elog(createView);
    }

    @Override
    public void visit(Alter alter) {
        blog(alter);
        super.visit(alter);
        elog(alter);
    }

    @Override
    public void visit(Statements stmts) {
        blog(stmts);
        super.visit(stmts);
        elog(stmts);
    }

    @Override
    public void visit(Execute execute) {
        blog(execute);
        super.visit(execute);
        elog(execute);
    }

    @Override
    public void visit(SetStatement set) {
        blog(set);
        super.visit(set);
        elog(set);
    }

    @Override
    public void visit(ShowColumnsStatement set) {
        blog(set);
        super.visit(set);
        elog(set);
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        blog(rowConstructor);
        super.visit(rowConstructor);
        elog(rowConstructor);
    }

    @Override
    public void visit(HexValue hexValue) {
        blog(hexValue);
        super.visit(hexValue);
        elog(hexValue);
    }

    @Override
    public void visit(Merge merge) {
        blog(merge);
        super.visit(merge);
        elog(merge);
    }

    @Override
    public void visit(OracleHint hint) {
        blog(hint);
        super.visit(hint);
        elog(hint);
    }

    @Override
    public void visit(TableFunction valuesList) {
        blog(valuesList);
        super.visit(valuesList);
        elog(valuesList);
    }

    @Override
    public void visit(AlterView alterView) {
        blog(alterView);
        super.visit(alterView);
        elog(alterView);
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        blog(timeKeyExpression);
        super.visit(timeKeyExpression);
        elog(timeKeyExpression);
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        blog(literal);
        super.visit(literal);
        elog(literal);
    }

    @Override
    public void visit(Commit commit) {
        blog(commit);
        super.visit(commit);
        elog(commit);
    }

    @Override
    public void visit(Upsert upsert) {
        blog(upsert);
        super.visit(upsert);
        elog(upsert);
    }

    @Override
    public void visit(UseStatement use) {
        blog(use);
        super.visit(use);
        elog(use);
    }

    @Override
    public void visit(ParenthesisFromItem parenthesis) {
        blog(parenthesis);
        super.visit(parenthesis);
        elog(parenthesis);
    }

    @Override
    public void visit(Block block) {
        blog(block);
        super.visit(block);
        elog(block);
    }

    @Override
    public void visit(Comment comment) {
        blog(comment);
        super.visit(comment);
        elog(comment);
    }

    @Override
    public void visit(ValuesStatement values) {
        blog(values);
        super.visit(values);
        elog(values);
    }

    @Override
    public void visit(DescribeStatement describe) {
        blog(describe);
        super.visit(describe);
        elog(describe);
    }

    @Override
    public void visit(ExplainStatement explain) {
        blog(explain);
        super.visit(explain);
        elog(explain);
    }

    @Override
    public void visit(NextValExpression nextVal) {
        blog(nextVal);
        super.visit(nextVal);
        elog(nextVal);
    }

    @Override
    public void visit(CollateExpression col) {
        blog(col);
        super.visit(col);
        elog(col);
    }

    @Override
    public void visit(ShowStatement aThis) {
        blog(aThis);
        super.visit(aThis);
        elog(aThis);
    }

    @Override
    public void visit(SimilarToExpression expr) {
        blog(expr);
        super.visit(expr);
        elog(expr);
    }

    @Override
    public void visit(DeclareStatement aThis) {
        blog(aThis);
        super.visit(aThis);
        elog(aThis);
    }

    @Override
    public void visit(ArrayExpression array) {
        blog(array);
        super.visit(array);
        elog(array);
    }
}
