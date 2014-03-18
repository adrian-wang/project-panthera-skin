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
package com.intel.ssg.dcst.panthera.parse.sql.transformer.fb.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.intel.ssg.dcst.panthera.parse.sql.PantheraExpParser;
import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateException;
import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateUtil;
import com.intel.ssg.dcst.panthera.parse.sql.TranslateContext;
import com.intel.ssg.dcst.panthera.parse.sql.transformer.fb.FilterBlockUtil;
import com.intel.ssg.dcst.panthera.parse.sql.transformer.fb.PLSQLFilterBlockFactory;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * provide common process logic for filter block processor. TODO<br>
 * <li>processCompareHavingUC and processEqualsUC didn¡¯t check whether single-row expression
 * returns only one row. It¡¯s ok for these 2 cases because sum and max always return one value.
 * Just a reminder don¡¯t miss the check part. If you didn¡¯t implement yet, add a ¡°TODO¡± remark
 * in code.<br>
 *
 * <li>there are some duplicate code need to be refactor.
 *
 * CommonFilterBlockProcessor.
 *
 */

public abstract class CommonFilterBlockProcessor extends BaseFilterBlockProcessor {

  CommonTree topAlias;
  CommonTree bottomAlias;
  CommonTree topTableRefElement;
  List<CommonTree> topAliasList;
  CommonTree closingSelect;
  CommonTree join;

  private static final Log LOG = LogFactory.getLog(CommonFilterBlockProcessor.class);

  /**
   * make branch for top select
   * @throws SqlXlateException
   */
  private void makeTop() throws SqlXlateException {

    // create top select table ref node
    topTableRefElement = super.createTableRefElement(topSelect);
    topAlias = (CommonTree) topTableRefElement.getChild(0);

    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    CommonTree topFrom = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);

    // create closing select
    closingSelect = super.createClosingSelect(topFrom, topTableRefElement);
  }

  /**
   * make branch join
   * add the join node to closingSelect from node, and add the bottomSelect to tableRefElement
   * which is new created and with new table alias added.
   *
   * @param joinType
   */
  private void makeJoin(CommonTree joinType) {
    // join
    join = super.createJoin(joinType, closingSelect);
    bottomAlias = super.buildJoin(joinType, join, bottomSelect);
  }

  /**
   * make branch for whole sub tree
   * @throws SqlXlateException
   */
  private void makeEnd() throws SqlXlateException {
    // closing select list
    CommonTree selectList = super.createSelectListForClosingSelect(topAlias, closingSelect, topAliasList);
    if (selectList.getChildCount() == 0) {
      selectList = FilterBlockUtil.cloneSelectListFromSelect((CommonTree) topSelect);
      for (int i = 0; i < selectList.getChildCount(); i++) {
        CommonTree selectItem = (CommonTree) selectList.getChild(i);
        CommonTree expr = (CommonTree) selectItem.getChild(0);
        expr.deleteChild(0);
        expr.addChild(super.createCascatedElement(FilterBlockUtil.cloneTree((CommonTree) selectItem
            .getChild(1).getChild(0))));
      }
    }
    closingSelect.addChild(selectList);
    FilterBlockUtil.AddAllNeededFilters(topSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);

    // set closing select to top select
    topSelect = closingSelect;
  }

  /**
   * process compare operator with uncorrelated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processCompareUC() throws SqlXlateException {

    // must have aggregation function in sub query
    this.processAggregationCompareUC();
  }

  private void processAggregationCompareUC() throws SqlXlateException {

    CommonTree compareElement = super.getSubQOpElement();

    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query compare with sub-query");
    }
    fbContext.getSelectStack().push(sq);

    this.makeTop();

    // add compare item
    CommonTree cloneCompareElement = FilterBlockUtil.cloneTree(compareElement);

    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), cloneCompareElement);
    super.rebuildSubQOpElement(compareElement, compareElementAlias);

    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK, "cross"));

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    this.makeEnd();

    // where
    super.buildWhereBranch(bottomAlias, comparSubqAlias);
  }

  /**
   * process compare operator with correlated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processCompareC() throws SqlXlateException {
    CommonTree compareElement = super.getSubQOpElement();
    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query compare with sub-query");
    }
    fbContext.getSelectStack().push(sq);

    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK, "cross"));

    CommonTree compareKeyAlias1 = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), FilterBlockUtil.cloneTree(compareElement));

    // select list
    assert(bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChildCount() == 1);
    CommonTree compareKeyAlias2 = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));
    super.rebuildSelectListByFilter(false, true, bottomAlias, topAlias);

    this.makeEnd();

    // where
    super.buildWhereByFB(subQNode, compareKeyAlias1, compareKeyAlias2);

  }

  /**
   * process IS_NULL/IS_NOT_NULL operator with uncorrelated<br>
   * (subQuery) is null, this case operator has only one child.
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processIsIsNotNullUC() throws SqlXlateException {

    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.LEFT_VK, "left"));

    // select list
    assert(bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChildCount() == 1);
    CommonTree compareKeyAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    this.makeEnd();

    // where
    CommonTree opBranch = super.buildWhereBranch(bottomAlias, compareKeyAlias);
    // for CrossjoinTransformer which should not optimize isNull/isNotNull node in WHERE.
    context.putBallToBasket(opBranch, true);

  }

  /**
   * process IS_NULL/IS_NOT_NULL operator with correlated<br>
   * (subQuery) is null, this case operator has only one child.
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processIsIsNotNullC() throws SqlXlateException {

    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.LEFT_VK, "left"));

    // select list
    assert(bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChildCount() == 1);
    CommonTree onlySelectItem = (CommonTree) ((CommonTree) bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0);
    CommonTree compareKeyAlias = super.addAlias(onlySelectItem);
    boolean needGroup = FilterBlockUtil.isAggrFunc(onlySelectItem);
    // if aggregation, then need group, else no need group
    // TODO is there any requirement for (SUBQUERY is null) filter? Should both be permitted?
    super.rebuildSelectListByFilter(false, needGroup, bottomAlias, topAlias);

    this.makeEnd();

    // where
    CommonTree opBranch = super.buildWhereByFB(subQNode, compareKeyAlias, null);
    // for CrossjoinTransformer which should not optimize isNull/isNotNull node in WHERE.
    context.putBallToBasket(opBranch, true);

  }

  /**
   * process exists with uncorrelated
   * When subQ is uncorrelated, then join the top and bottom query with left semi join.
   * Once the results of subQ is not empty, the the topQ will output all the table by left semi join.
   *
   * @throws SqlXlateException
   */
  public void processExistsUC() throws SqlXlateException {
    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.LEFTSEMI_VK, "leftsemi"));
    this.makeEnd();
  }

  /**
   * process exists with correlated
   *
   * @throws SqlXlateException
   */
  void processExistsC() throws SqlXlateException {
    this.makeTop();


    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK,
        PantheraExpParser.LEFTSEMI_STR));


    super.processSelectAsterisk(bottomSelect);
    super.rebuildSelectListByFilter(false, false, bottomAlias, topAlias);

    this.makeEnd();
    super.buildWhereByFB(null, null, null);

    if (super.hasNotEqualCorrelated) {
      // become inner join if there is not equal correlated.
      join.deleteChild(0);
      // add distinct
      SqlXlateUtil.addCommonTreeChild(this.closingSelect, 1, FilterBlockUtil.createSqlASTNode(
          subQNode, PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT, "distinct"));
    }
  }

  /**
   * process not exists for uncorrelated
   * <li>not Exists will be transformed to cross join, the select list of bottom select
   * will be set as count(*). And the filter for cross join will be output of bottom select
   * equals to 0. Namely Count(*)=0.
   *
   * @throws SqlXlateException
   */
  public void processNotExistsUC() throws SqlXlateException {
    this.makeTop();
    bottomSelect = super.reCreateBottomSelect(bottomSelect, super.createTableRefElement(bottomSelect), super
        .createCountAsteriskSelectList());
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.CROSS_VK, "cross"));
    this.makeEnd();
    super.reBuildNotExist4UCWhere(subQNode.parent, topSelect, (CommonTree) this.bottomAlias.getChild(0));
  }

  /**
   * process not exists with correlated
   * not exists will be transform to exists first
   * and use topselect results minus the EXISTS results
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processNotExistsC() throws SqlXlateException {
    // treat not exists as exists first, and make exists transformation (left semi)
    this.makeTop();
    // restore the original topSelect
    CommonTree cloneOriginTopSelect = FilterBlockUtil.cloneTree(topSelect);
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK,
        PantheraExpParser.LEFTSEMI_STR));
    super.processSelectAsterisk(bottomSelect);
    super.rebuildSelectListByFilter(false, false, bottomAlias, topAlias);
    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    // clone the topSelect for later use
    CommonTree cloneMinusTopSelect = FilterBlockUtil.cloneTree(topSelect);

    // top will become closing after makeEnd
    CommonTree rememberTop = topSelect;
    this.makeEnd();
    // add again here
    FilterBlockUtil.AddAllNeededFilters(cloneMinusTopSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);
    CommonTree cloneMinusList = (CommonTree) cloneMinusTopSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (cloneMinusList != null) {
      for (int index = 0; index < cloneMinusList.getChildCount(); index++) {
        cloneMinusList.replaceChildren(index, index, FilterBlockUtil.cloneTree((CommonTree) rememberTop.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChild(index)));
      }
    }
    // add alias origin
    FilterBlockUtil.addColumnAliasOrigin(cloneMinusTopSelect, context);

    // add again here
    FilterBlockUtil.AddAllNeededFilters(topSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);

    super.buildWhereByFB(null, null, null);

    if (super.hasNotEqualCorrelated) {
      // become inner join if there is not equal correlated.
      join.deleteChild(0);
      // add distinct
      SqlXlateUtil.addCommonTreeChild(this.closingSelect, 1, FilterBlockUtil.createSqlASTNode(
          subQNode, PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT, "distinct"));
    }

    // exists subQ transform finished, start to make minus operation
    CommonTree minus = FilterBlockUtil.createSqlASTNode(topSelect,
        PantheraParser_PLSQLParser.PLSQL_RESERVED_MINUS, "minus");
    // topSelect here is EXISTS subQ transformed select
    minus.addChild(topSelect);

    // create top select table ref node
    topTableRefElement = super.createTableRefElement(cloneMinusTopSelect);
    cloneMinusTopSelect.getParent().addChild(minus);
    topAlias = (CommonTree) topTableRefElement.getChild(0);

    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) cloneOriginTopSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    CommonTree oldFrom = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    // create closing select
    closingSelect = super.createClosingSelect(oldFrom, topTableRefElement);
    CommonTree selectList = super.createSelectListForClosingSelect(topAlias, closingSelect, topAliasList);
    closingSelect.addChild(selectList);
    topSelect = closingSelect;
  }

  /**
   * process not exists with correlated
   *
   * This method for processing NOT EXISTS is used in Facebook transformed TPCH-21 and TPCH-22.
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processNotExistsCByLeftJoin() throws SqlXlateException {
    this.makeTop();
    super.joinTypeNode = FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK,
        PantheraExpParser.LEFT_STR);
    this.makeJoin(joinTypeNode);

    // for optimizing not equal condition
    // Map<joinType node,List<not equal condition node>>
    Map<CommonTree, List<CommonTree>> joinMap = (Map<CommonTree, List<CommonTree>>) super.context
        .getBallFromBasket(TranslateContext.JOIN_TYPE_NODE_BALL);
    if (joinMap == null) {
      joinMap = new HashMap<CommonTree, List<CommonTree>>();
      super.context.putBallToBasket(TranslateContext.JOIN_TYPE_NODE_BALL, joinMap);
    }

    joinMap.put(joinTypeNode, new ArrayList<CommonTree>());

    super.processSelectAsterisk(bottomSelect);
    //for not exists correlated subquery, origin column in select list is meaningless, remove it;
    CommonTree selectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    while (selectList.getChildCount() != 0) {
      selectList.deleteChild(0);
    }

    super.rebuildSelectListByFilter(true, false, bottomAlias, topAlias);
    this.makeEnd();
    super.buildWhereByFB(null, null, null);
  }

  /**
   * process in with uncorrelated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processInUC() throws SqlXlateException {

    CommonTree compareElement = super.getSubQOpElement();

    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query as IN sub-query node");
    }
    fbContext.getSelectStack().push(sq);

    this.makeTop();

    // add compare item
    // TODO multi parameter in sub query IN
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), super.cloneSubQOpElement());

    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.LEFTSEMI_VK, "leftsemi"));

    // compare alias from subq
    if (bottomSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST) == null) {
      throw new SqlXlateException(bottomSelect, "No select-list or select * in subquery!");
    }
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    // on
    // FIXME which is first?
    CommonTree equal = FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.EQUALS_OP, "=");
    CommonTree on = super.buildOn(equal, super.createCascatedElementWithTableName(equal,
        (CommonTree) topAlias.getChild(0), (CommonTree) compareElementAlias.getChild(0)), super
        .createCascatedElementWithTableName(equal, (CommonTree) bottomAlias.getChild(0),
            (CommonTree) comparSubqAlias.getChild(0)));
    join.addChild(on);

    this.makeEnd();
  }

  /**
   * process IN with correlated
   *
   * @throws SqlXlateException
   */
  void processInC() throws SqlXlateException {
    CommonTree compareElement = super.getSubQOpElement();

    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query as IN sub-query node");
    }
    fbContext.getSelectStack().push(sq);

    this.makeTop();

    // add compare item
    // TODO multi parameter in sub query IN
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), super.cloneSubQOpElement());

    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.LEFTSEMI_VK, "leftsemi"));

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    // on
    CommonTree equal = FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.EQUALS_OP, "=");
    CommonTree leftChild = compareElementAlias.getType() == PantheraParser_PLSQLParser.ALIAS ? super
        .createCascatedElementWithTableName(equal, (CommonTree) topAlias.getChild(0),
            (CommonTree) compareElementAlias.getChild(0))
        : compareElementAlias;
    CommonTree on = super.buildOn(equal, leftChild, super
        .createCascatedElementWithTableName(equal, (CommonTree) bottomAlias.getChild(0),
            (CommonTree) comparSubqAlias.getChild(0)));
    join.addChild(on);

    super.rebuildSelectListByFilter(false, false, bottomAlias, topAlias);
    this.makeEnd();
    super.buildWhereByFB(null, null, null);
  }

  /**
   * process not in with uncorrelated
   * <li>TODO
   * <li>NOT IN UC is currently transform into collect_set and array_Contains. Which is not
   * able to handly NULL condition.
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processNotInUC() throws SqlXlateException {
    CommonTree compareElement = super.getSubQOpElement();

    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), compareElement) != 0) {
      throw new SqlXlateException(compareElement, "not support element from outter query as NOT_IN sub-query node");
    }
    fbContext.getSelectStack().push(sq);

    CommonTree joinType = FilterBlockUtil.createSqlASTNode(subQNode, PantheraParser_PLSQLParser.CROSS_VK,
        "cross");
    this.makeTop();

    // add compare item
    List<CommonTree> compareElementAlias = super.addSelectItems4In(topSelect, super.subQNode);

    this.makeJoin(joinType);

    CommonTree selectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    // compare alias from subq
    List<CommonTree> comparSubqAlias = null;
    // FIXME: this method would not support NULL condition
    if (!super.needAddOneLevel4CollectSet(selectList)) {
      super.rebuildCollectSet();
      comparSubqAlias = super.buildSelectListAlias(bottomAlias, selectList);
    } else { // need to add one SELECT level for collect_set
      comparSubqAlias = super.buildSelectListAlias(bottomAlias, selectList);
      CommonTree newLevelSelect = super.addOneLevel4CollectSet(bottomSelect, bottomAlias, comparSubqAlias);
      bottomSelect.getParent().replaceChildren(bottomSelect.getChildIndex(), bottomSelect.getChildIndex(), newLevelSelect);
      bottomSelect = newLevelSelect;
      super.rebuildCollectSet();
    }

    this.makeEnd();

    // where
    // FIXME which is first?
    CommonTree where = super.buildWhere(FilterBlockUtil.dupNode(subQNode), comparSubqAlias,
        compareElementAlias, bottomAlias, topAlias);
    closingSelect.addChild(where);
    super.rebuildArrayContains((CommonTree) where.getChild(0));
  }

  /**
   * process not in with correlated
   * NOT IN correlated will be transformed into NOT EXISTS
   *
   * @throws SqlXlateException
   */
  void processNotInC() throws SqlXlateException {
    CommonTree leftIn = super.getSubQOpElement();
    // check if leftIn is from the upper by 1 level.
    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), leftIn) != 0) {
      throw new SqlXlateException(leftIn, "not support element from outter query as NOT_IN sub-query node");
    }
    fbContext.getSelectStack().push(sq);
    super.buildAnyElement(leftIn, topSelect, false);

    if (bottomSelect.getFirstChildWithType(PantheraExpParser.SELECT_LIST).getChildCount() > 1) {
      throw new SqlXlateException((CommonTree) bottomSelect.getFirstChildWithType(PantheraExpParser
          .SELECT_LIST), "NOT_IN subQuery select-list should have only one column");
    }
    CommonTree rightIn = (CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST)).getChild(0).getChild(0).getChild(0);
    // rightIn do not have to check level
    super.buildAnyElement(rightIn, bottomSelect, true);

    CommonTree equal = FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.EQUALS_OP, "=");
    equal.addChild(leftIn);
    equal.addChild(rightIn);

    CommonTree topand = FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.SQL92_RESERVED_AND, "and");
    topand.addChild(equal);
    topand.addChild(fb.getASTNode());
    fb.setASTNode(topand);

    LOG.info("Transform NOT IN to NOT EXISTS:"
        + topand.toStringTree().replace('(', '[').replace(')', ']'));

    CommonTree selectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    int index = selectList.getChildIndex();
    bottomSelect.deleteChild(index);
    SqlXlateUtil.addCommonTreeChild(bottomSelect, index, FilterBlockUtil.createSqlASTNode(
        subQNode, PantheraExpParser.ASTERISK, "*"));

    /*
     * NOT IN correlated subQ will transformed into NOT EXISTS correlated subQ
     * IMPORTANT: The following two line use the two method to handle NOT EXISTS Correlated subQ
     * IMPORTANT: Here is a switch to use either method. Just Comment one line will use another method.
     * "processNotExistsCByLeftJoin" function use the method like faceBook transformed TPC-H Q21 and Q22
     * "processNotExistsC" use the method in ASE design document, which will transformed into EXISTS and use MINUS to reduce the result.
     */
    //super.processNotExistsCByLeftJoin();
    this.processNotExistsC();
  }

  /**
   * for topSelect contains NULL in ALL subQuery
   * if bottomSelect generates nothing, keep this NULL
   * else not keep the NULL
   *
   * @param leftOp
   * @throws SqlXlateException
   */
  public void processAllWindUp(CommonTree leftOp, CommonTree simpleTop) throws SqlXlateException {
    CommonTree cloneAlmostTopSelect = FilterBlockUtil.cloneTree(topSelect);
    // use oldest top select to avoid duplicate calculate
    // if HIVE can cache old subQueries in one query, this would be a bad tradeoff
    topSelect = simpleTop;
    // start to build the exceptional cases query
    this.makeTop();
    // use condition panthera_col_0>0
    bottomSelect = super.reCreateBottomSelect(bottomSelect, super.createTableRefElement(bottomSelect), super
        .createCountAsteriskSelectList());
    this.makeJoin(FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.CROSS_VK, "cross"));
    this.makeEnd();
    // where is (panthera_col_0>0 and leftOp is null)
    // ignore the possible correlated condition, since it is a minus
    // no need to add DISTINCT since the right table only gets one row.
    CommonTree where = super.buildWhereForAll(leftOp);
    closingSelect.addChild(where);

    // exceptional subQ transform finished, start to make minus operation
    CommonTree minus = FilterBlockUtil.createSqlASTNode(topSelect,
        PantheraParser_PLSQLParser.PLSQL_RESERVED_MINUS, "minus");
    // topSelect here is EXISTS subQ transformed select
    minus.addChild(topSelect);
    // add needed filters for cloneAlmostTopSelect
    FilterBlockUtil.AddAllNeededFilters(topSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);

    // create top select table ref node
    topTableRefElement = super.createTableRefElement(cloneAlmostTopSelect);
    cloneAlmostTopSelect.getParent().addChild(minus);
    topAlias = (CommonTree) topTableRefElement.getChild(0);

    // add alias origin
    FilterBlockUtil.addColumnAliasOrigin(cloneAlmostTopSelect, context);

    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) cloneAlmostTopSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    // add needed filters for cloneAlmostTopSelect
    FilterBlockUtil.AddAllNeededFilters(cloneAlmostTopSelect, topQuery.isProcessHaving(), whereFilterColumns, havingFilterColumns);

    CommonTree oldFrom = (CommonTree) topSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    // create closing select
    closingSelect = super.createClosingSelect(oldFrom, topTableRefElement);
    CommonTree selectList = super.createSelectListForClosingSelect(topAlias, closingSelect, topAliasList);
    closingSelect.addChild(selectList);
    topSelect = closingSelect;
  }

}
