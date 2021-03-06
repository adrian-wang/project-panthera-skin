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
package com.intel.ssg.dcst.panthera.parse.sql.generator;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateException;
import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateUtil;
import com.intel.ssg.dcst.panthera.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Generator for substring() standard function.
 *
 */
public class SubstringFuncGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    assert(currentSqlNode.getType() == PantheraParser_PLSQLParser.SUBSTRING_VK);

    //
    // Translate the substring SQL node to a HIVE Identifier node whose text is "substring" and attach it
    // to currentHiveNode as the first child.
    //
    ASTNode funcName = SqlXlateUtil.newASTNode(HiveParser.Identifier, "substring");
    attachHiveNode(hiveRoot, currentHiveNode, funcName);
    //
    // The children (arguments for substring function) of this substring SQL node will be translated into
    // other children of the current HIVE node.
    //
    return super.generateChildren(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode, context);
  }

}
