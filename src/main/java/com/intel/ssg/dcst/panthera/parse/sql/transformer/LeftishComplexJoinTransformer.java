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

import org.antlr.runtime.tree.CommonTree;

import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateException;
import com.intel.ssg.dcst.panthera.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * enabling (A join B) join C
 * LeftishComplexJoinTransformer.
 *
 */
public class LeftishComplexJoinTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public LeftishComplexJoinTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  private void trans(CommonTree node, TranslateContext context) throws SqlXlateException {
    for (int i = 0; i < node.getChildCount(); i++) {
      trans((CommonTree) node.getChild(i), context);
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

}
