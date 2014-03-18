/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.ssg.dcst.panthera.parse.sql.transformer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;

import com.intel.ssg.dcst.panthera.parse.sql.PantheraExpParser;
import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateException;
import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateUtil;
import com.intel.ssg.dcst.panthera.parse.sql.TranslateContext;
import com.intel.ssg.dcst.panthera.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * deal with USING/NATURAL JOIN/nested JOIN
 *
 * ComplexJoinTransformer.
 *
 */
public class ComplexJoinTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public ComplexJoinTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    // omit the root node of the tree
    trans((CommonTree) tree.getChild(0), context);
  }

  private void trans(CommonTree node, TranslateContext context) throws SqlXlateException {
    firstTrans(node, context);
    oldTrans(node, context);
  }

  /**
   * eliminate first child of join-ref being JOIN, mainly for speed.
   *
   * @param node
   * @param context
   * @throws SqlXlateException
   */
  private void firstTrans(CommonTree node, TranslateContext context) throws SqlXlateException {
    for (int i = 0; i < node.getChildCount(); i++) {
      firstTrans((CommonTree) node.getChild(i), context);
    }
    if (node.getType() == PantheraParser_PLSQLParser.TABLE_REF) {
      // only deal the first child of every table_ref since this is firstTrans()
      CommonTree element = (CommonTree) node.getChild(0);
      assert(element.getType() == PantheraParser_PLSQLParser.TABLE_REF_ELEMENT);
      // two situations for table_ref_element
      // 1) table_expression, or alias and table_expression
      // 2) only one child, and is table_ref
      if (element.getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_REF) != null) {
        // first table_ref_element of node(table_ref) is eligible to optimize
        assert (element.getChildCount() == 1);
        CommonTree splitTableRef = (CommonTree) element.deleteChild(0);
        splitTableRef.token = null;
        node.replaceChildren(0, 0, splitTableRef);
      }
    }
  }

  private void oldTrans(CommonTree node, TranslateContext context) throws SqlXlateException {
    int childCount = node.getChildCount();
    for (int i = 0; i < childCount; i++) {
      oldTrans((CommonTree) node.getChild(i), context);
    }
    if (node.getType() == PantheraExpParser.TABLE_REF
        && node.getParent().getType() == PantheraExpParser.TABLE_REF_ELEMENT) {
      processNested(node, context);
    }
    if (node.getType() == PantheraExpParser.PLSQL_NON_RESERVED_USING) {
      CommonTree join = (CommonTree) node.getParent();
      if (join.getType() != PantheraExpParser.JOIN_DEF) {
        throw new SqlXlateException(join, "Unsupported USING type:" + join.getText());
      }
      processUsing(node, context);
    }
    if (node.getType() == PantheraExpParser.TABLE_REF && node.getChildCount() > 1
        && node.getChild(1).getType() == PantheraExpParser.JOIN_DEF
        && node.getChild(1).getChild(0).getType() == PantheraExpParser.NATURAL_VK) {
      Map<String, Set<String>> columnSetMap = processNaturalJoin(node, context);
      // take INNER as LEFT, because when INNER join on left & right equal, neither cannot be null.
      int joinType = PantheraParser_PLSQLParser.LEFT_VK;
      if (node.getChild(1).getChild(0).getType() == PantheraParser_PLSQLParser.RIGHT_VK
          || node.getChild(1).getChild(0).getType() == PantheraParser_PLSQLParser.FULL_VK) {
        joinType = node.getChild(1).getChild(0).getType();
      }
      // parent of table_ref can also be table_ref_element
      CommonTree select = (CommonTree) node.getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
      Map<String, CommonTree> commonMap = rebuildSelectNatural(select, columnSetMap, joinType);
      FilterBlockUtil.rebuildColumn((CommonTree) select
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE), commonMap);
      FilterBlockUtil.rebuildColumn((CommonTree) select
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP), commonMap);
      FilterBlockUtil.deleteAllTableAlias((CommonTree) ((CommonTree) (select.getParent().getParent()))
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ORDER));
    }
  }

  private void processNested(CommonTree node, TranslateContext context) throws SqlXlateException {
    CommonTree select = (CommonTree) node.getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    CommonTree closingTabRefElement = (CommonTree) node.getParent();
    CommonTree closingTabExpression = FilterBlockUtil.createSqlASTNode(closingTabRefElement,
        PantheraParser_PLSQLParser.TABLE_EXPRESSION, "TABLE_EXPRESSION");
    closingTabRefElement.replaceChildren(node.childIndex, node.childIndex, closingTabExpression);
    CommonTree closingSelectMode = FilterBlockUtil.createSqlASTNode(closingTabRefElement,
        PantheraParser_PLSQLParser.SELECT_MODE, "SELECT_MODE");
    closingTabExpression.addChild(closingSelectMode);
    CommonTree closingSelectStatement = FilterBlockUtil.createSqlASTNode(closingTabRefElement,
        PantheraParser_PLSQLParser.SELECT_STATEMENT, "SELECT_STATEMENT");
    closingSelectMode.addChild(closingSelectStatement);
    CommonTree closingSubquery = FilterBlockUtil.createSqlASTNode(closingTabRefElement,
        PantheraParser_PLSQLParser.SUBQUERY, "SUBQUERY");
    closingSelectStatement.addChild(closingSubquery);
    CommonTree closingSelect = FilterBlockUtil.createSqlASTNode(closingTabRefElement,
        PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    closingSubquery.addChild(closingSelect);
    // FIXME here the from is made out from air, must have many problems, ahhh
    CommonTree closingFrom = FilterBlockUtil.createSqlASTNode(closingTabRefElement,
        PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    closingSelect.addChild(closingFrom);
    closingFrom.addChild(node);
    CommonTree asterisk = FilterBlockUtil.createSqlASTNode(closingTabRefElement, PantheraParser_PLSQLParser.ASTERISK, "*");
    closingSelect.addChild(asterisk);

//    // rebuild select-list
//    Map<String, CommonTree> commonMap = rebuildSelectNested(select, context);
//    // rebuild others
//    FilterBlockUtil.rebuildColumn((CommonTree) select
//        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE), commonMap);
//    FilterBlockUtil.rebuildColumn((CommonTree) select
//        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP), commonMap);
//    FilterBlockUtil.deleteAllTableAlias((CommonTree) ((CommonTree) (select.getParent().getParent()))
//        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ORDER));
  }

  private void processUsing(CommonTree node, TranslateContext context) throws SqlXlateException {
    CommonTree join = (CommonTree) node.getParent();
    CommonTree tableRef = (CommonTree) join.getParent();
    assert (join.childIndex > 0);
    CommonTree leftTableRefElement = (CommonTree) tableRef.getChild(join.childIndex - 1);
    if (leftTableRefElement.getType() != PantheraExpParser.TABLE_REF_ELEMENT) {
      throw new SqlXlateException(join, "currently only support USING specified in first join of a TABLE_REF");
    }
    // parent of table_ref can also be table_ref_element
    CommonTree select = (CommonTree) node.getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    CommonTree rightTableRefElement = (CommonTree) join
        .getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT);
    String leftTableName = SqlXlateUtil.findTableReferenceName(leftTableRefElement);
    String rightTableName = SqlXlateUtil.findTableReferenceName(rightTableRefElement);
    List<String> leftColumnList = new ArrayList<String>();
    List<String> rightColumnList = new ArrayList<String>();
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree columnName = (CommonTree) node.getChild(i);
      String column = columnName.getChild(0).getText();
      leftColumnList.add(column);
      rightColumnList.add(column);
    }
    assert (join.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ON) == null);
    // build on to replace USING
    CommonTree on = FilterBlockUtil.makeOn(node, leftTableName, rightTableName, leftColumnList,
        rightColumnList);
    join.replaceChildren(node.childIndex, node.childIndex, on);

    // take INNER as LEFT, because when INNER join on left & right equal, neither cannot be null.
    int joinType = PantheraParser_PLSQLParser.LEFT_VK;
    if (join.getChild(0).getType() == PantheraParser_PLSQLParser.RIGHT_VK
        || join.getChild(0).getType() == PantheraParser_PLSQLParser.FULL_VK) {
      joinType = join.getChild(0).getType();
    }

    // rebuild select-list
    Map<String, CommonTree> commonMap = rebuildSelectUsing(select, node, joinType, leftTableRefElement, rightTableRefElement, context);
    // rebuild others
    FilterBlockUtil.rebuildColumn((CommonTree) select
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE), commonMap);
    FilterBlockUtil.rebuildColumn((CommonTree) select
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP), commonMap);
    FilterBlockUtil.deleteAllTableAlias((CommonTree) ((CommonTree) (select.getParent().getParent()))
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ORDER));
  }

  /**
   *
   * 1. expand select * if exists, omit USING cols, use common colname as alias.<br>
   * 2. replace table alias in select-list
   * 3. replace upper order by x.a with order by a
   * 4. replace filters of a to x.a
   *
   * @param select
   * @param columnSetMap
   * @param joinType
   * @return commonMap
   * @throws SqlXlateException
   */
  private Map<String, CommonTree> rebuildSelectUsing(CommonTree select, CommonTree node,
      int joinType, CommonTree leftTable, CommonTree rightTable, TranslateContext context) throws SqlXlateException {
    CommonTree selectList = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    CommonTree asterisk = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK);
    CommonTree newSelectList = FilterBlockUtil.createSqlASTNode(asterisk != null ? asterisk : selectList,
        PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
    Map<String, CommonTree> commonMap = rebuildAsteriskUsing(node, newSelectList, joinType, leftTable, rightTable, context);
    if (asterisk != null) {
      if (selectList == null) {
        assert(asterisk.childIndex == 1);
        select.replaceChildren(asterisk.childIndex, asterisk.childIndex, newSelectList);
        selectList = newSelectList;
      } else if (asterisk.childIndex == 2) {
        for (int i = 0; i < newSelectList.getChildCount(); i++) {
          selectList.addChild(newSelectList.getChild(i));
        }
      } else {
        for (int i = 0; i < newSelectList.getChildCount(); i++) {
          SqlXlateUtil.addCommonTreeChild(selectList, i, (CommonTree) newSelectList.getChild(i));
        }
      }
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      List<CommonTree> anyList = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(selectItem, PantheraExpParser.ANY_ELEMENT, anyList);
      if (anyList.size() == 0) {
        continue;
      }
      for (CommonTree anyElement : anyList) {
        String colname = anyElement.getChild(anyElement.getChildCount() - 1).getText();
        if (selectItem.getChildCount() == 1
            && selectItem.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
            && selectItem.getChild(0).getChild(0).getChild(0) == anyElement) {
          // if no alias user defined, rewrite it as col name to ensure dup col be deleted.
          selectItem.addChild(FilterBlockUtil.createAlias(selectItem, colname));
        }
        FilterBlockUtil.rebuildColumn(anyElement, commonMap);
      }
    }
    return commonMap;
  }

  private Map<String, CommonTree> rebuildAsteriskUsing(CommonTree node, CommonTree selectList,
      int joinType, CommonTree leftTable, CommonTree rightTable, TranslateContext context) throws SqlXlateException {
    Map<String, CommonTree> commonMap = new HashMap<String, CommonTree>();
    Set<String> set = new HashSet<String>();
    String leftTableName = SqlXlateUtil.findTableReferenceName(leftTable);
    String rightTableName = SqlXlateUtil.findTableReferenceName(rightTable);
    for (int i = 0; i < node.getChildCount(); i++) {
      String columnName = node.getChild(i).getChild(0).getText();
      CommonTree selectItem = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
      CommonTree expr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
      selectItem.addChild(expr);
      if (joinType == PantheraParser_PLSQLParser.LEFT_VK) {
        CommonTree cascated = FilterBlockUtil.createCascatedElementBranch(selectList, leftTableName, columnName);
        expr.addChild(cascated);
      } else if (joinType == PantheraParser_PLSQLParser.RIGHT_VK) {
        CommonTree cascated = FilterBlockUtil.createCascatedElementBranch(selectList, rightTableName, columnName);
        expr.addChild(cascated);
      } else {
        CommonTree leftCascated = FilterBlockUtil.createCascatedElementBranch(selectList, leftTableName, columnName);
        CommonTree rightCascated = FilterBlockUtil.createCascatedElementBranch(selectList, leftTableName, columnName);
        CommonTree search = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SEARCHED_CASE, "case");
        expr.addChild(search);
        CommonTree tokenWhen = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SQL92_RESERVED_WHEN, "when");
        CommonTree tokenElse = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SQL92_RESERVED_ELSE, "else");
        search.addChild(tokenWhen);
        search.addChild(tokenElse);
        CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
        tokenWhen.addChild(logicExpr);
        CommonTree isNull = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.IS_NULL, "IS_NULL");
        logicExpr.addChild(isNull);
        isNull.addChild(FilterBlockUtil.cloneTree(FilterBlockUtil.cloneTree(rightCascated)));
        CommonTree leftExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
        CommonTree rightExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
        tokenElse.addChild(rightExpr);
        tokenWhen.addChild(leftExpr);
        leftExpr.addChild(leftCascated);
        rightExpr.addChild(rightCascated);
      }
      CommonTree alias = FilterBlockUtil.createAlias(selectList, columnName);
      selectItem.addChild(alias);
      selectList.addChild(selectItem);
      commonMap.put(columnName, selectItem);
      set.add(columnName);
    }
    addRemainingCols(selectList, leftTable, leftTableName, set, context);
    addRemainingCols(selectList, rightTable, rightTableName, set, context);
    return commonMap;
  }

  private void addRemainingCols(CommonTree selectList, CommonTree table, String tableName, Set<String> set, TranslateContext context) throws SqlXlateException {
    Set<String> colSet = FilterBlockUtil.getColumnSet(table, context);
    Iterator<String> colIt = colSet.iterator();
    while (colIt.hasNext()) {
      String col = colIt.next();
      if (set.contains(col)) {
        continue;
      }
      CommonTree selectItem = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
      CommonTree expr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
      selectItem.addChild(expr);
      CommonTree cascated = FilterBlockUtil.createCascatedElementBranch(selectList, tableName, col);
      expr.addChild(cascated);
      CommonTree alias = FilterBlockUtil.createAlias(selectList, col);
      selectItem.addChild(alias);
      selectList.addChild(selectItem);
    }

  }

  /**
   *
   * 1. expand select * if exists, omit dup col, use common colname as alias.<br>
   * 2. replace table alias in select-list
   * 3. replace upper order by x.a with order by a
   * 4. replace filters of a to x.a
   *
   * @param select
   * @param columnSetMap
   * @param joinType
   * @return commonMap
   */
  private Map<String, CommonTree> rebuildSelectNatural(CommonTree select, Map<String, Set<String>> columnSetMap, int joinType) {
    CommonTree selectList = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    CommonTree asterisk = (CommonTree) select.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK);
    CommonTree newSelectList = FilterBlockUtil.createSqlASTNode(asterisk != null ? asterisk : selectList, PantheraParser_PLSQLParser.SELECT_LIST, "SELECT_LIST");
    Map<String, CommonTree> commonMap = rebuildAsteriskNatural(newSelectList, columnSetMap, joinType);
    if (asterisk != null) {
      if (selectList == null) {
        assert(asterisk.childIndex == 1);
        select.replaceChildren(asterisk.childIndex, asterisk.childIndex, newSelectList);
        selectList = newSelectList;
      } else if (asterisk.childIndex == 2) {
        for (int i = 0; i < newSelectList.getChildCount(); i++) {
          selectList.addChild(newSelectList.getChild(i));
        }
      } else {
        for (int i = 0; i < newSelectList.getChildCount(); i++) {
          SqlXlateUtil.addCommonTreeChild(selectList, i, (CommonTree) newSelectList.getChild(i));
        }
      }
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      List<CommonTree> anyList = new ArrayList<CommonTree>();
      FilterBlockUtil.findNode(selectItem, PantheraExpParser.ANY_ELEMENT, anyList);
      if (anyList.size() == 0) {
        continue;
      }
      for (CommonTree anyElement : anyList) {
        String colname = anyElement.getChild(anyElement.getChildCount() - 1).getText();
       if (selectItem.getChildCount() == 1
            && selectItem.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT
            && selectItem.getChild(0).getChild(0).getChild(0) == anyElement) {
          // if no alias user defined, rewrite it as col name to ensure dup col be deleted.
          selectItem.addChild(FilterBlockUtil.createAlias(selectItem, colname));
        }
        FilterBlockUtil.rebuildColumn(anyElement, commonMap);
      }
    }
    return commonMap;
  }

  private Map<String, CommonTree> rebuildAsteriskNatural(CommonTree selectList, Map<String, Set<String>> columnSetMap,
      int joinType) {
    Iterator<Entry<String, Set<String>>> it = columnSetMap.entrySet().iterator();
    Set<String> set = new HashSet<String>();
    Map<String, CommonTree> index = new HashMap<String, CommonTree>();
    Map<String, CommonTree> commonMap = new HashMap<String, CommonTree>();
    Stack<Entry<String, Set<String>>> entries = new Stack<Entry<String, Set<String>>>();
    int commonCount = 0;
    while (it.hasNext()) {
      entries.push(it.next());
    }
    // use stack to first do right table and then left table
    while (entries.size() > 0) {
      Entry<String, Set<String>> entry = entries.pop();
      String table = entry.getKey();
      Set<String> val = entry.getValue();
      Iterator<String> colIter = val.iterator();
      int independentCount = commonCount;
      while (colIter.hasNext()) {
        String col = colIter.next();
        CommonTree cascated = FilterBlockUtil.createCascatedElementBranch(selectList, table, col);
        CommonTree expr = FilterBlockUtil.createSqlASTNode(cascated, PantheraParser_PLSQLParser.EXPR, "EXPR");
        expr.addChild(cascated);
        CommonTree alias = FilterBlockUtil.createAlias(cascated, col);
        CommonTree selectItem = FilterBlockUtil.createSqlASTNode(expr, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
        selectItem.addChild(expr);
        selectItem.addChild(alias);
        if (set.contains(col)) {
          // this is a shared column, put it at first of selectList
          CommonTree existSelectItem = index.get(col);
          // here in the same table will not have two cols with same colname
          // TODO need check
          existSelectItem.getParent().deleteChild(existSelectItem.getChildIndex());
          if (commonMap.get(col) != null) {
            commonCount--;
            independentCount--;
          }
          if (joinType == PantheraParser_PLSQLParser.LEFT_VK) {
            SqlXlateUtil.addCommonTreeChild(selectList, commonCount++, selectItem);
            commonMap.put(col, selectItem);
          } else if (joinType == PantheraParser_PLSQLParser.RIGHT_VK) {
            SqlXlateUtil.addCommonTreeChild(selectList, commonCount++, existSelectItem);
            commonMap.put(col, existSelectItem);
          } else {
            // natural full outer join
            // (case when t1.a is null then t2.a else t1.a end)
            CommonTree compositeSelectItem = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SELECT_ITEM, "SELECT_ITEM");
            CommonTree compositeExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.EXPR, "EXPR");
            compositeSelectItem.addChild(compositeExpr);
            compositeSelectItem.addChild((CommonTree) selectItem.deleteChild(1));
            CommonTree search = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SEARCHED_CASE, "case");
            compositeExpr.addChild(search);
            CommonTree tokenWhen = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SQL92_RESERVED_WHEN, "when");
            CommonTree tokenElse = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.SQL92_RESERVED_ELSE, "else");
            search.addChild(tokenWhen);
            search.addChild(tokenElse);
            CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
            tokenWhen.addChild(logicExpr);
            CommonTree isNull = FilterBlockUtil.createSqlASTNode(selectList, PantheraParser_PLSQLParser.IS_NULL, "IS_NULL");
            logicExpr.addChild(isNull);
            isNull.addChild(FilterBlockUtil.cloneTree((CommonTree) selectItem.getChild(0).getChild(0)));
            tokenElse.addChild((CommonTree) selectItem.deleteChild(0));
            tokenWhen.addChild((CommonTree) existSelectItem.deleteChild(0));
            SqlXlateUtil.addCommonTreeChild(selectList, commonCount++, compositeSelectItem);
            commonMap.put(col, compositeSelectItem);
          }
          independentCount++;
        } else {
          SqlXlateUtil.addCommonTreeChild(selectList, independentCount++, selectItem);
          set.add(col);
          index.put(col, selectItem);
        }
      }
    }
    return commonMap;
  }

  /**
   * change natural join to equijoin
   *
   * @param node
   * @param context
   * @return return a map for sets of all table_ref_element, key is table alias,
   * val is a set for all cols.
   * @throws SqlXlateException
   */
  private Map<String, Set<String>> processNaturalJoin(CommonTree node, TranslateContext context)
      throws SqlXlateException {
    Map<String, Set<String>> columnSetMap = new LinkedHashMap<String, Set<String>>();
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree child = (CommonTree) node.getChild(i);
      if (child.getType() == PantheraExpParser.TABLE_REF_ELEMENT) {
        String leftTable = SqlXlateUtil.findTableReferenceName(child);
        columnSetMap.put(leftTable, FilterBlockUtil.getColumnSet(child, context));
      }
      if (child.getType() == PantheraExpParser.JOIN_DEF
          && child.getChild(0).getType() == PantheraExpParser.NATURAL_VK) {
        // change join type
        CommonTree natural = processNaturalNode(child);
        CommonTree tableRefElement = (CommonTree) child
            .getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT);
        String alias = SqlXlateUtil.findTableReferenceName(tableRefElement);
        Set<String> columnSet = FilterBlockUtil.getColumnSet(tableRefElement, context);
        makeOn(natural, alias, child, columnSet, columnSetMap);
        columnSetMap.put(alias, columnSet);
      }
    }
    return columnSetMap;
  }

  /**
   * make equijoin condition
   *
   * @param njoin
   * @param thisTable
   * @param join
   * @param thisColumnSet
   * @param columnSetMap
   */
  private void makeOn(CommonTree natural, String thisTable, CommonTree join, Set<String> thisColumnSet,
      Map<String, Set<String>> columnSetMap) {
    CommonTree on = FilterBlockUtil.createSqlASTNode(natural,
        PantheraExpParser.SQL92_RESERVED_ON, "on");//njoin.getChild(0) is natural_vk
    CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(on, PantheraExpParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    on.addChild(logicExpr);
    boolean hasCondition = false;
    for (String column : thisColumnSet) {
      for (Entry<String, Set<String>> entry : columnSetMap.entrySet()) {
        Set<String> preColumnSet = entry.getValue();
        String tableAlias = entry.getKey();
        if (preColumnSet.contains(column)) {
          hasCondition = true;
          CommonTree equal = FilterBlockUtil.makeEqualCondition(on, thisTable, tableAlias, column, column);
          FilterBlockUtil.addConditionToLogicExpr(logicExpr, equal);
          break;
        }
      }
    }
    if (hasCondition) {
      join.addChild(on);
    }
  }

  /**
   * build columnSetMap and delete INNER node
   *
   * @param join
   * @return natural join node
   */
  private CommonTree processNaturalNode(CommonTree join) {
    // delete NATURAL node
    CommonTree natural = (CommonTree) join.deleteChild(0);
    if (join.getChild(0).getType() == PantheraExpParser.INNER_VK) {
      // delete INNER node
      join.deleteChild(0);
    }
    return natural;
  }

}
