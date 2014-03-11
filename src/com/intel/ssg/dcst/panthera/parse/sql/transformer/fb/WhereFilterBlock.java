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
package com.intel.ssg.dcst.panthera.parse.sql.transformer.fb;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;

import com.intel.ssg.dcst.panthera.parse.sql.PantheraConstants;
import com.intel.ssg.dcst.panthera.parse.sql.PantheraExpParser;
import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateException;
import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateUtil;
import com.intel.ssg.dcst.panthera.parse.sql.TranslateContext;
import com.intel.ssg.dcst.panthera.parse.sql.transformer.fb.QueryBlock.CountAsterisk;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public class WhereFilterBlock extends TypeFilterBlock {
  private CommonTree subtable;

  public CommonTree getSubTable() {
    return subtable;
  }

  /*
   * 1. make sure group by elements will always appear in select list and
   * init the QueryBlock of current WhereFilterBlock
   * 2. init the queryBlock contain current whereFilterBlock, especially preprocess
   * the aggregations in select list.
   */
  @Override
  void preExecute(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    groupCount = addGroupElementToSelectList(fbContext, context);
    //when finished dealing from-subq and begin to deal where, topSelect should refresh.
    fbContext.getQueryStack().peek().init();
    fbContext.getQueryStack().peek().setHaving(false);
  }

  /*
   * 1. add one level if there is group by expression and subQ in where clause.
   * 2. remove elements in select list which are inserted from group by element in preExecute process.
   * 3. restore aggregation funcs which is stored in preExecute process, for QueryBlock init().
   * 4. refresh and add the group branch if there is to the added level select.
   */
  @Override
  void execute(FilterBlockContext fbContext, TranslateContext context) throws SqlXlateException {
    rebuildSelectListAddingLevel(fbContext, context);
    removeGroupElementFromSelectList(fbContext, context);
    restoreAggregationFunction(fbContext, context);
    refreshGroup();
  }

  /**
   * To end the process of where, and if there is group, then need to add a
   * level for group, group shall be processed for transformed query instead
   * of the original one.
   * @param fbContext
   * @param context
   * @throws SqlXlateException
   */
  private void rebuildSelectListAddingLevel(FilterBlockContext fbContext,
      TranslateContext context) throws SqlXlateException {
    CommonTree select = (CommonTree) this.getASTNode().getParent();
    CommonTree group = (CommonTree) select.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP);
    CommonTree transformedNode = this.getTransformedNode();
    // only when there is subQ in where clause and there is group by clause
    // will need to add one level to rebuild select list.
    if (transformedNode == null || group == null) {
      return ;
    }
    if(select.getCharPositionInLine() != this.getTransformedNode().getCharPositionInLine()) {
      return ;
    }
    CommonTree newTree = FilterBlockUtil.createSqlASTNode(transformedNode, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    CommonTree leftSelect = transformedNode;
    while (leftSelect.getType() != PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      leftSelect = (CommonTree) leftSelect.getChild(0);
    }
    CommonTree subq = FilterBlockUtil.makeSelectBranch(newTree, context,
        (CommonTree) leftSelect.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
    this.subtable = (CommonTree) newTree.getChild(0).getChild(0).getChild(0).getChild(0).getChild(0);
    if (transformedNode.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      subq.getParent().replaceChildren(subq.childIndex, subq.childIndex, transformedNode);
    } else {
      assert(transformedNode.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
      subq.addChild(transformedNode);
    }

    // selectList could not be null
    // if select * where easy, not come here because transformed is null
    // if select * where subQ, will expand asterisk.
    CommonTree selectList = FilterBlockUtil.cloneSelectListFromSelect(leftSelect);
    List <CommonTree> anyList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(selectList, PantheraParser_PLSQLParser.ANY_ELEMENT, anyList);
    for (CommonTree any:anyList) {
      if (any.getChildCount() == 2) {
        any.replaceChildren(0, 0, FilterBlockUtil.createSqlASTNode(any, PantheraParser_PLSQLParser.ID, subtable.getText()));
      }
    }
    newTree.addChild(selectList);
    this.setTransformedNode(newTree);
  }

  private void restoreAggregationFunction(FilterBlockContext fbContext, TranslateContext context) throws SqlXlateException {
    if(this.getTransformedNode() == null) {
      return;
    }
    QueryBlock qb = fbContext.getQueryStack().peek();
    // common aggregation funcs, like avg/max/min/count(col) etc.
    List<CommonTree> aggregationList = qb.getAggregationList();
    // count(*) & count(integer)
    CountAsterisk countAsterisk = qb.getCountAsterisk();

    // restore aggregation function
    CommonTree transformed = this.getTransformedNode();
    doRestoreAggr(transformed, aggregationList, countAsterisk);
  }

  private void doRestoreAggr(CommonTree transformed, List<CommonTree> aggregationList, CountAsterisk countAsterisk) throws SqlXlateException {
    if (transformed.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      doRestoreAggregation(transformed, aggregationList, countAsterisk);
    } else if (transformed.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_UNION) {
      doRestoreAggr((CommonTree) transformed.getChild(0), aggregationList, countAsterisk);
    } else if (transformed.getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      for (int i = 0; i < transformed.getChildCount(); i++) {
        doRestoreAggr((CommonTree) transformed.getChild(i), aggregationList, countAsterisk);
      }
    }
  }

  private void doRestoreAggregation(CommonTree select, List<CommonTree> aggregationList, CountAsterisk countAsterisk) throws SqlXlateException {
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    // select *, don't need to restore aggregation funcs.
    if (selectList == null && select.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK) != null) {
      return;
    }
    if (countAsterisk.isOnlyAsterisk()) {
      //if countAsterisk.isOnlyAsterisk is true, countAsterisk.selectItem can't be null
      //must be CommonTree instead of BaseTree. BaseTree.getCharPositionInLine() always return 0.
      if(((CommonTree) countAsterisk.getSelectItem().getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT)).
          getCharPositionInLine() == select.getCharPositionInLine()) {
        selectList = null;
      }
    }

    // recover the aggregation func in select List in the corresponding position
    if (selectList != null && aggregationList != null) {
      if (selectList.getChildCount() + groupCount != aggregationList.size()) {
        throw new SqlXlateException(selectList, "mismatch select item's size after transformed.");
      }
      for (int i = 0; i < aggregationList.size(); i++) {
        CommonTree func = aggregationList.get(i);
        if (func != null) {
          CommonTree selectItem = (CommonTree) selectList.getChild(i);
          CommonTree expr = (CommonTree) selectItem.getChild(0);
          CommonTree cascatedElement = (CommonTree) expr.deleteChild(0);
          expr.addChild(func);
          List<CommonTree> exprList = new ArrayList<CommonTree>();
          FilterBlockUtil.findNode(func, PantheraParser_PLSQLParser.EXPR, exprList);
          CommonTree expr2 = exprList.get(0);
          expr2.addChild(cascatedElement);
        }
      }
    }
    // count(*) in top query
    if (countAsterisk.getSelectItem() != null) {
      // count(*) is the only select item in original query
      if (selectList == null) {
        int position;
        if (select.getFirstChildWithType(PantheraExpParser.ASTERISK) != null) {
          position = select.getFirstChildWithType(PantheraExpParser.ASTERISK).getChildIndex();
        } else if (select.getFirstChildWithType(PantheraExpParser.SELECT_LIST) != null) {
          position = select.getFirstChildWithType(PantheraExpParser.SELECT_LIST).getChildIndex();
        } else {
          throw new SqlXlateException(select, "No select list");
        }
        select.deleteChild(position);
        selectList = FilterBlockUtil.createSqlASTNode(countAsterisk
            .getSelectItem(), PantheraExpParser.SELECT_LIST,
            "SELECT_LIST");
        SqlXlateUtil.addCommonTreeChild(select, position, selectList);

      }

      if( ((CommonTree) countAsterisk.getSelectItem().getAncestor(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT))
          .getCharPositionInLine() == select.getCharPositionInLine()) {
        SqlXlateUtil.addCommonTreeChild(selectList, countAsterisk.getPosition(), countAsterisk
          .getSelectItem());
      }
    }

    // after insert count(*), give aliases begin with PantheraConstants.PANTHERA_AGGR to all standard function
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      if (FilterBlockUtil.findOnlyNode(selectItem, PantheraParser_PLSQLParser.STANDARD_FUNCTION) == null) {
        continue;
      }
      CommonTree rebuildAlias = (CommonTree) selectItem.getChild(1);
      if (rebuildAlias != null) {
        ((CommonTree) rebuildAlias.getChild(0)).getToken().setText(PantheraConstants.PANTHERA_AGGR + i);
      } else {
        rebuildAlias = FilterBlockUtil.createSqlASTNode(selectItem, PantheraParser_PLSQLParser.ALIAS, "ALIAS");
        CommonTree rebuildID = FilterBlockUtil.createSqlASTNode(rebuildAlias, PantheraParser_PLSQLParser.ID,
            PantheraConstants.PANTHERA_AGGR + i);
        rebuildAlias.addChild(rebuildID);
        selectItem.addChild(rebuildAlias);
      }
    }
    return;
  }

  /**
   * refresh group with new table name if there is table name node under ANY_ELEMENT, and
   * add the group branch to the new add select level.
   */
  private void refreshGroup() {
    CommonTree select = (CommonTree) this.getASTNode().getParent();
    CommonTree group = (CommonTree) select.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_GROUP);
    // there is group by expression and SubQ in where clause in original query.
    if(group == null || this.getTransformedNode() == null) {
      return;
    }
    if(select.getCharPositionInLine() != this.getTransformedNode().getCharPositionInLine()) {
      return;
    }
    List <CommonTree> anyList = new ArrayList<CommonTree>();
    CommonTree having = (CommonTree) group.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);
    // remove having branch if exists, having will be added a new level for processing.
    if (having != null) {
      group.deleteChild(having.childIndex);
    }
    FilterBlockUtil.findNode(group, PantheraParser_PLSQLParser.ANY_ELEMENT, anyList);
    for (CommonTree any:anyList) {
      if (any.getChildCount() == 2) {
        any.replaceChildren(0, 0, FilterBlockUtil.createSqlASTNode(any, PantheraParser_PLSQLParser.ID, subtable.getText()));
      }
    }
    CommonTree simpleGroup = FilterBlockUtil.cloneTree(group);
    group.addChild(having);
    // TODO fix user defined alias problem.
    CommonTree transSelect = this.getTransformedNode();
    transSelect.addChild(simpleGroup);
  }

}
