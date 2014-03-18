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
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;

import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateException;
import com.intel.ssg.dcst.panthera.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class SubQFilterBlock extends BaseFilterBlock {
  @Override
  public void prepare(FilterBlockContext fbContext, TranslateContext context, Stack<CommonTree> selectStack)
      throws SqlXlateException {
    CommonTree subQ = this.getASTNode();
    prepareChildren(fbContext, context, selectStack);
    if (subQ.getChildCount() > 1) {
      List<CommonTree> nodeList = new ArrayList<CommonTree>();
      int element = subQ.getChild(0).getType() == PantheraParser_PLSQLParser.SUBQUERY ? 1 : 0;
      FilterBlockUtil.findNode((CommonTree) subQ.getChild(element), PantheraParser_PLSQLParser.CASCATED_ELEMENT, nodeList);
      for (CommonTree node : nodeList) {
        if (node.getChild(0).getType() == PantheraParser_PLSQLParser.ROUTINE_CALL) {
          continue;
        }
        int level = PLSQLFilterBlockFactory.getInstance().isCorrelated(fbContext.getqInfo(), selectStack, (CommonTree) node);
        Stack<QueryBlock> tempQS = new Stack<QueryBlock>();
        Stack<TypeFilterBlock> tempTS = new Stack<TypeFilterBlock>();
        for (int i = 0; i < level; i++) {
          tempQS.push(fbContext.getQueryStack().pop());
          tempTS.push(fbContext.getTypeStack().pop());
        }
        // both correlated and uncorrelated columns will be stored in QueryBlock.
        /*
         * here use add(FilterBlockUtil.cloneTree(node)) will detach the relation of "node" with WhereFilterColumns
         * because the column in WHERE will not be refreshed.
         */
        fbContext.getQueryStack().peek().getWhereFilterColumns().add(FilterBlockUtil.cloneTree(node));
        for (int i = 0; i < level; i++) {
          fbContext.getTypeStack().push(tempTS.pop());
          fbContext.getQueryStack().push(tempQS.pop());
        }
      }

      nodeList = new ArrayList<CommonTree>();
      // for aggregation funcs, if it has correlated columns, need to store this function into QueryBlcok.
      FilterBlockUtil.findNode((CommonTree) subQ.getChild(element), PantheraParser_PLSQLParser.STANDARD_FUNCTION, nodeList);
      for (CommonTree node : nodeList) {
        // TODO: there might be other non aggregation functions have STANDARD_FUNCTION node.
        if (node.getChild(0).getText().equals("substring") || node.getChild(0).getText().equals("substr")) {
          continue;
        }
        CommonTree cas = FilterBlockUtil.findOnlyNode(node, PantheraParser_PLSQLParser.CASCATED_ELEMENT);
        int level;
        if (cas != null) {
          level = PLSQLFilterBlockFactory.getInstance().isCorrelated(fbContext.getqInfo(), selectStack, cas);
        } else {
          level = 0;
        }
        Stack<QueryBlock> tempQS = new Stack<QueryBlock>();
        Stack<TypeFilterBlock> tempTS = new Stack<TypeFilterBlock>();
        for (int i = 0; i < level; i++) {
          tempQS.push(fbContext.getQueryStack().pop());
          tempTS.push(fbContext.getTypeStack().pop());
        }
        // store aggregation into HavingFilterColumns. Aggregation function can not exists in WHERE clause.
        // so the aggregation founded must be from HAVING clause.
        /*
         * here use "add(node)" instead of "add(FilterBlockUtil.cloneTree(node))", because the column name will be
         * refreshed in HAVING clause
         */
        fbContext.getQueryStack().peek().getHavingFilterColumns().add(node);
        for (int i = 0; i < level; i++) {
          fbContext.getTypeStack().push(tempTS.pop());
          fbContext.getQueryStack().push(tempQS.pop());
        }
      }
    }
  }

  @Override
  void processChildren(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    CommonTree subQ = FilterBlockUtil.createSqlASTNode(this.getASTNode(), PantheraParser_PLSQLParser.SUBQUERY, "SUBQUERY");
    this.setTransformedNode(subQ);
    for (FilterBlock fb : this.getChildren()) {
      fb.process(fbContext, context);
      // TODO temporarily workaround without setFB
      if (subQ.getChildCount() == 0) {
        subQ.addChild(fb.getTransformedNode());
        subQ.getToken().setCharPositionInLine(fb.getTransformedNode().getCharPositionInLine());
      } else {
        CommonTree union = FilterBlockUtil.createSqlASTNode(this.getASTNode(), PantheraParser_PLSQLParser.SQL92_RESERVED_UNION, "union");
        union.addChild(fb.getTransformedNode());
        subQ.addChild(union);
      }
    }
  }

  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    fbContext.getSubQStack().push(this);
    processChildren(fbContext, context);
    fbContext.getSubQStack().pop();
    postExecute(fbContext, context);
  }

  /**
   * update transformed node of fiter blocks in context stack and
   * handle condition of SUBQUERY for transformed node (against selectStack).
   *
   * @param fbContext
   * @param context
   */
  private void postExecute(FilterBlockContext fbContext, TranslateContext context) {
    fbContext.getTypeStack().peek().setTransformedNode(this.getTransformedNode());
    fbContext.getQueryStack().peek().setQueryForTransfer(this.getTransformedNode());
    if (this.getTransformedNode().getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      // TODO transformed node children more than one is not good
      fbContext.getSelectStackForTransfer().pop();
      CommonTree select = FilterBlockUtil.findOnlyNode(this.getTransformedNode(), PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
      fbContext.getSelectStackForTransfer().push(select);
    }
  }

}
