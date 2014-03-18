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

public class TrimGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    // node trim is a node of "trim"
    ASTNode trim = SqlXlateUtil.newASTNode(HiveParser.Identifier, currentSqlNode.getText());
    int count = currentSqlNode.getChildCount();
    if (count == 1) {
      super.attachHiveNode(hiveRoot, currentHiveNode, trim);
      return super.generateChildren(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode, context);
    } else {
      assert (count > 1);
      // trim ('x' from column)
      // trim (leading from column)
      // trim (trailing 'x' from column)
      // these cases all have FROM as the last second child of TRIM
      assert (currentSqlNode.getChild(count - 2).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      CommonTree range = (CommonTree) currentSqlNode.getChild(0);
      CommonTree node = (CommonTree) currentSqlNode.getChild(count - 1);
      if (range.getType() == PantheraParser_PLSQLParser.BOTH_VK
          || range.getType() == PantheraParser_PLSQLParser.LEADING_VK
          || range.getType() == PantheraParser_PLSQLParser.TRAILING_VK) {
        // range is set
        switch (range.getType()) {
        case PantheraParser_PLSQLParser.LEADING_VK:
          trim.token.setText("ltrim");
          break;
        case PantheraParser_PLSQLParser.TRAILING_VK:
          trim.token.setText("rtrim");
          break;
        }
        assert (count == 3 || count == 4);
        if (count == 3) {
          // trim (trailing from column)
          super.attachHiveNode(hiveRoot, currentHiveNode, trim);
          return GeneratorFactory.getGenerator(node).generateHiveAST(hiveRoot, sqlRoot,
              currentHiveNode, node, context);
        } else {
          // trim (both 'x' from column)
          CommonTree trimChar = (CommonTree) currentSqlNode.getChild(1);
          return generateAdvancedTrim(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode, trim,
              trimChar, node, context);
        }
      } else {
        assert (count == 3);
        // trim ('x' from column)
        CommonTree trimChar = (CommonTree) currentSqlNode.getChild(0);
        return generateAdvancedTrim(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode, trim,
            trimChar, node, context);
      }
    }
  }

  private boolean generateAdvancedTrim(ASTNode hiveRoot, CommonTree sqlRoot,
      ASTNode currentHiveNode, CommonTree currentSqlNode, ASTNode trim, CommonTree trimChar,
      CommonTree trimHandle, TranslateContext context) throws SqlXlateException {
    // use '^A' the control character as the temporary char
    String protectChar = "'" + String.valueOf('\10') + "'";
    ASTNode replaceUProtect = SqlXlateUtil.newASTNode(HiveParser.Identifier, "regexp_replace");
    ASTNode replaceRecover = SqlXlateUtil.newASTNode(HiveParser.Identifier, "regexp_replace");
    ASTNode replacePrepare = SqlXlateUtil.newASTNode(HiveParser.Identifier, "regexp_replace");
    ASTNode replaceProtect = SqlXlateUtil.newASTNode(HiveParser.Identifier, "regexp_replace");
    ASTNode functionRecover = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    ASTNode functionTrim = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    ASTNode functionPrepare = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    ASTNode functionProtect = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    super.attachHiveNode(hiveRoot, currentHiveNode, replaceUProtect);
    super.attachHiveNode(hiveRoot, currentHiveNode, functionRecover);
    ASTNode uProtect = SqlXlateUtil.newASTNode(HiveParser.StringLiteral, protectChar);
    ASTNode spaceUProtect = SqlXlateUtil.newASTNode(HiveParser.StringLiteral, "' '");
    super.attachHiveNode(hiveRoot, currentHiveNode, uProtect);
    super.attachHiveNode(hiveRoot, currentHiveNode, spaceUProtect);
    super.attachHiveNode(hiveRoot, functionRecover, replaceRecover);
    super.attachHiveNode(hiveRoot, functionRecover, functionTrim);
    ASTNode spaceRecover = SqlXlateUtil.newASTNode(HiveParser.StringLiteral, "' '");
    super.attachHiveNode(hiveRoot, functionRecover, spaceRecover);
    if (!GeneratorFactory.getGenerator(trimChar).generateHiveAST(hiveRoot, sqlRoot,
        functionRecover, trimChar, context)) {
      return false;
    }
    super.attachHiveNode(hiveRoot, functionTrim, trim);
    super.attachHiveNode(hiveRoot, functionTrim, functionPrepare);
    super.attachHiveNode(hiveRoot, functionPrepare, replacePrepare);
    super.attachHiveNode(hiveRoot, functionPrepare, functionProtect);
    if (!GeneratorFactory.getGenerator(trimChar).generateHiveAST(hiveRoot, sqlRoot,
        functionPrepare, trimChar, context)) {
      return false;
    }
    ASTNode spacePrepare = SqlXlateUtil.newASTNode(HiveParser.StringLiteral, "' '");
    super.attachHiveNode(hiveRoot, functionPrepare, spacePrepare);
    super.attachHiveNode(hiveRoot, functionProtect, replaceProtect);
    if (!GeneratorFactory.getGenerator(trimHandle).generateHiveAST(hiveRoot, sqlRoot,
        functionProtect, trimHandle, context)) {
      return false;
    }
    ASTNode spaceProtect = SqlXlateUtil.newASTNode(HiveParser.StringLiteral, "' '");
    ASTNode protect = SqlXlateUtil.newASTNode(HiveParser.StringLiteral, protectChar);
    super.attachHiveNode(hiveRoot, functionProtect, spaceProtect);
    super.attachHiveNode(hiveRoot, functionProtect, protect);
    return true;
  }

}
