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
package org.apache.hadoop.hive.ql.parse.sql.transformer;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.QueryBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.SubQFilterBlock;

/**
 * Transform filter block tree with every QueryInfo.
 *
 * SubQUnnestTransformer.
 *
 */
public class SubQUnnestTransformer extends BaseSqlASTTransformer {

  private static final Log LOG = LogFactory.getLog(SubQUnnestTransformer.class);
  SqlASTTransformer tf;

  public SubQUnnestTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformQInfoDeepFirst(tree, context);
  }

  /**
   * Transform inline view firstly.
   *
   * @param tree
   * @param context
   * @throws SqlXlateException
   */
  void transformQInfoDeepFirst(CommonTree tree, TranslateContext context) throws SqlXlateException {
    QueryInfo qInfo = context.getQInfoRoot();
    transformQInfoDeepFirst(qInfo, context);
  }

  void transformQInfoDeepFirst(QueryInfo qf, TranslateContext context) throws SqlXlateException {

    for (QueryInfo qinfo : qf.getChildren()) {
      transformQInfoDeepFirst(qinfo, context);
    }
    if (!qf.isQInfoTreeRoot()) {
      this.transformFilterBlock(qf, context);
    } else {
      // do nothing
    }
  }

  void transformFilterBlock(QueryInfo qf, TranslateContext context) throws SqlXlateException {
    if (!this.hasSubQuery(qf)) {
      LOG.info("skip subq transform:" + qf.toStringTree());
      return;
    }
    FilterBlock fb = qf.getFilterBlockTreeRoot();
    if (!(fb instanceof QueryBlock)) {
      throw new SqlXlateException(null, "Error FilterBlock tree" + fb.toStringTree());
    }
    // prefetch columns from filterBlocks and store them in QueryBlock in FilterBLockContext.
    fb.prepare(new FilterBlockContext(qf), context, new Stack<CommonTree>());
    // process QueryBlock
    fb.process(new FilterBlockContext(qf), context);
  }

  /**
   * check whether this QueryBlock have SubQFilterBlock.
   * @param qf
   * @return
   */
  boolean hasSubQuery(QueryInfo qf) {
    FilterBlock fb = qf.getFilterBlockTreeRoot();
    Set<Boolean> result = new HashSet<Boolean>();
    this.isSubQFilterBlock(fb, result);
    if (result.contains(true)) {
      return true;
    }
    return false;
  }

  void isSubQFilterBlock(FilterBlock fb, Set<Boolean> result) {
    if (fb instanceof SubQFilterBlock) {
      result.add(true);
      return;
    }
    for (FilterBlock child : fb.getChildren()) {
      isSubQFilterBlock(child, result);
    }
  }
}
