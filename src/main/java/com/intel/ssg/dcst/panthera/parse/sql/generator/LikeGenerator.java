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
import com.intel.ssg.dcst.panthera.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class LikeGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    // handle escape first if there is.
    preprocessEscape(currentSqlNode);
    return super.baseProcess(HiveParser.KW_LIKE, "like", hiveRoot, sqlRoot, currentHiveNode, currentSqlNode, context);
  }

  /**
   * process the escape clause first and delete the escape branch after substitute the format string in LIKE clause.
   * TODO: only support "LIKE '***' escape '*'", not support "LIKE COL escape '*'"
   *
   * @param currentSqlNode
   *        the LIKE node
   * @throws SqlXlateException
   */
  public void preprocessEscape(CommonTree currentSqlNode) throws SqlXlateException {
    if (currentSqlNode.getChildCount() >= 2) {
      CommonTree likeStrNode = (CommonTree) currentSqlNode.getChild(1).getChild(0);
      // like '***' escape '*'
      if (currentSqlNode.getChildCount() == 3) {
        // don't support column after LIKE.
        // like COL escape '*'
        if (likeStrNode.getType() != PantheraParser_PLSQLParser.CHAR_STRING) {
          throw new SqlXlateException(currentSqlNode, "Panthera do not support Column name after like when escape characters is specified.");
        }
        CommonTree escapeCharNode = (CommonTree) currentSqlNode.getChild(2).getChild(0);
        if (escapeCharNode.getType() != PantheraParser_PLSQLParser.CHAR_STRING ||
            escapeCharNode.getText().length() != 3) { // the character must be in format '*', 3 character in total.
          throw new SqlXlateException(currentSqlNode, "Invalid parameter after ESCAPE, it should be a character.");
        }
        String likeStr = likeStrNode.getText();
        char escapeChar = escapeCharNode.getText().charAt(1);
        // replace the string with escape character
        String replacedStr = replaceWithEscape(currentSqlNode, likeStr, escapeChar);
        likeStrNode.getToken().setText(replacedStr);
        // delete the escape character branch
        currentSqlNode.deleteChild(2);
      } else if (currentSqlNode.getChildCount() == 2) {
        if (likeStrNode.getType() == PantheraParser_PLSQLParser.CHAR_STRING) {
          String likeStrTmp = likeStrNode.getText();
          StringBuilder replaceStr = new StringBuilder();
          for (int i = 0; i < likeStrTmp.length(); i++) {
            if (likeStrTmp.charAt(i) == '\\') {
              // here need to give warning for user that LIKE format is according to ORACLE
              // System.out.println("WARNING: ...");
              // need add an extra '\\' to the original String.
              replaceStr.append(likeStrTmp.charAt(i));
            }
            // just copy the original string
            replaceStr.append(likeStrTmp.charAt(i));
          }
          likeStrNode.getToken().setText(new String(replaceStr));
        }
      }
    }
  }

  /**
   * replace the escapeChar in the string to be escaped
   *
   * @param currentSqlNode
   * @param toEscapedStr
   *        string to be escaped
   * @param escapeChar
   * @return
   *        return the string that has be substituted by the escaped char.
   * @throws SqlXlateException
   */
  private String replaceWithEscape(CommonTree currentSqlNode, String toEscapedStr, char escapeChar) throws SqlXlateException {
    StringBuilder ret = new StringBuilder();
    for (int i = 0; i < toEscapedStr.length(); i++) {
      char currentChar = toEscapedStr.charAt(i);
      // '\\' is not an escape character in ORACLE, but is a default escape character in HIVE,
      // so need to add additional '\\' to it to avoid escaping in HIVE. e.g. 'a\b' ---> 'a\\b'
      if (escapeChar != '\\' && currentChar == '\\') {
        // here need to give warning for user that LIKE format is according to ORACLE
        // System.out.println("WARNING: ...");
        // need add an extra '\\' to the original String.
        ret.append('\\').append(currentChar);
        continue;
      }
      // skip the character that is not the same with escapeChar.
      if (currentChar != escapeChar) {
        ret.append(currentChar);
        continue;
      }
      char escapedChar = toEscapedStr.charAt(i+1);
      // only the character following is '_' or '%' will be escaped with '\\', other cases will just eliminate the current character
      // e.g. like 'abc_' escape 'c'  --->  like 'ab\_'
      // TODO: may be there are other characters need tobe escaped with '\\' not only '_' and '%'.
      if (escapedChar == '_' || escapedChar == '%') {
        ret.append('\\');
      } else {
        // if the escape character is not repeated, throw exception
        if (currentChar != escapedChar) {
          throw new SqlXlateException(currentSqlNode, "invalid escape format.");
        }
        //here skip the escape character
      }
      // append the following character
      ret.append(toEscapedStr.charAt(++i));
    }
    return new String(ret);
  }

}
