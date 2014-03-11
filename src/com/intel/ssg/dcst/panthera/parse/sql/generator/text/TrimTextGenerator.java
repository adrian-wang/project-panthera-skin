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
package com.intel.ssg.dcst.panthera.parse.sql.generator.text;

import org.antlr.runtime.tree.CommonTree;

import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateException;
import com.intel.ssg.dcst.panthera.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * generate like trim(both 'X' from column) .<br>
 * TrimTextGenerator.
 *
 */
public class TrimTextGenerator extends BaseTextGenerator {

  @Override
  protected String textGenerate(CommonTree root, TranslateContext context) throws Exception {
    int count = root.getChildCount();
    CommonTree range = (CommonTree) root.getChild(0);
    // store column name
    String trimCol = super.textGenerateChildIndex(root, context, count - 1);
    if (count == 1) {
      return "trim(" + trimCol + ")";
    } else if (count == 3) {
      switch (range.getType()) {
      // trim (leading from column)
      case PantheraParser_PLSQLParser.LEADING_VK:
        return "ltrim(" + trimCol + ")";
      case PantheraParser_PLSQLParser.TRAILING_VK:
        return "rtrim(" + trimCol + ")";
      case PantheraParser_PLSQLParser.BOTH_VK:
        return "trim(" + trimCol + ")";
      default:
        // trim ('X' from column)
        String trimStr = super.textGenerateChildIndex(root, context, count - 3);
        return textGenerateAdvancedTrim("trim", trimStr, trimCol);
      }
    } else if (count == 4) {
      // trim (leading 'x' from column)
      String trimStr = super.textGenerateChildIndex(root, context, count - 3);
      switch (range.getType()) {
      // trim (leading from column)
      case PantheraParser_PLSQLParser.LEADING_VK:
        return textGenerateAdvancedTrim("ltrim", trimStr, trimCol);
      case PantheraParser_PLSQLParser.TRAILING_VK:
        return textGenerateAdvancedTrim("rtrim", trimStr, trimCol);
      default:
        assert (range.getType() == PantheraParser_PLSQLParser.BOTH_VK);
        return textGenerateAdvancedTrim("trim", trimStr, trimCol);
      }
    } else {
      throw new SqlXlateException(root, "Unsupported TRIM type");
    }
  }

  private String textGenerateAdvancedTrim(String trim, String trimStr, String trimCol)
      throws SqlXlateException {
    // use '^A' the control character as the temporary char
    String protectChar = "'" + String.valueOf('\10') + "'";
    return "regexp_replace(regexp_replace(" + trim + "(regexp_replace(regexp_replace(" + trimCol
        + ", ' ', " + protectChar + "), " + trimStr + ", ' ')), ' ', " + trimStr + "), "
        + protectChar + ", ' ')";
  }
}
