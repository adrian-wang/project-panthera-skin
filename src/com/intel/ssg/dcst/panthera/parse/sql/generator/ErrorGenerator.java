package com.intel.ssg.dcst.panthera.parse.sql.generator;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import com.intel.ssg.dcst.panthera.parse.sql.SqlXlateException;
import com.intel.ssg.dcst.panthera.parse.sql.TranslateContext;

public class ErrorGenerator extends BaseHiveASTGenerator{


  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    // log error
    return false;
  }

}
