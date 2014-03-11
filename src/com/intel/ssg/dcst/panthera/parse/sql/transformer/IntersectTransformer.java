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

import java.util.List;

import org.antlr.runtime.tree.CommonTree;

import com.intel.ssg.dcst.panthera.parse.sql.PantheraExpParser;
import com.intel.ssg.dcst.panthera.parse.sql.transformer.fb.FilterBlockUtil;

/**
 * transform INTERSECT to LEFTSEMI JOIN.
 *
 * IntersectTransformer.
 *
 */
public class IntersectTransformer extends SetOperatorTransformer {

  @Override
  CommonTree makeJoinNode(CommonTree on) {
    CommonTree join = FilterBlockUtil.createSqlASTNode(on, PantheraExpParser.JOIN_DEF, "join");
    join.addChild(FilterBlockUtil.createSqlASTNode(on, PantheraExpParser.LEFTSEMI_VK, "leftsemi"));
    return join;
  }

  @Override
  CommonTree makeWhere(CommonTree setOperator, CommonTree leftTableRefElement, CommonTree rightTableRefElement,
      List<CommonTree> leftColumnAliasList, List<CommonTree> rightColumnAliasList) {
    return null;
  }
}
