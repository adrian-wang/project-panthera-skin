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

/**
 * generate like Function(...) .<br>
 * FuncTextGenerator.
 *
 */
public class FuncTextGenerator extends BaseTextGenerator {

  @Override
  protected String textGenerate(CommonTree root, TranslateContext context) throws Exception {
    if (root.getChildCount() < 2) {
      return root.getText() + "(" + textGenerateChild(root, context) + ")";
    } else if (root.getChildCount() == 2) {  // e.g. select max(col1) over() from ...
      CommonTree op1 = (CommonTree) root.getChild(0);
      QueryTextGenerator qr1 = TextGeneratorFactory.getTextGenerator(op1);

      CommonTree op2 = (CommonTree) root.getChild(1);
      QueryTextGenerator qr2 = TextGeneratorFactory.getTextGenerator(op2);
      return root.getText() + "(" + qr1.textGenerateQuery(op1, context) + ") "
          + qr2.textGenerateQuery(op2, context);
    } else {
      throw new SqlXlateException(root, "Unsupported function type!");
    }
  }

}
