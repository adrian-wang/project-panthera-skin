/**
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

package com.intel.ssg.dcst.panthera.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jline.ArgumentCompletor;
import jline.ArgumentCompletor.AbstractArgumentDelimiter;
import jline.ArgumentCompletor.ArgumentDelimiter;
import jline.Completor;
import jline.ConsoleReader;
import jline.History;
import jline.SimpleCompletor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.OptionsProcessor;
import org.apache.hadoop.hive.common.HiveInterruptUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.IOUtils;
import org.apache.thrift.TException;

import sun.misc.Signal;
import sun.misc.SignalHandler;


/**
 * PantheraCliDriver.
 *
 */
public class PantheraCliDriver extends CliDriver {

	private final LogHelper console;
  private Configuration conf;

  public PantheraCliDriver() {
    SessionState ss = SessionState.get();
    conf = (ss != null) ? ss.getConf() : new Configuration();
    Log LOG = LogFactory.getLog("PantheraCliDriver");
    console = new LogHelper(LOG);
  }
  
	int processLocalCmd(String cmd, CommandProcessor proc, CliSessionState ss) {
    int tryCount = 0;
    boolean needRetry;
    int ret = 0;

    do {
      try {
        needRetry = false;
        if (proc != null) {
          if (proc instanceof Driver) {
            SkinDriver qp = (SkinDriver) proc;
            PrintStream out = ss.out;
            long start = System.currentTimeMillis();
            if (ss.getIsVerbose()) {
              out.println(cmd);
            }

            qp.setTryCount(tryCount);
            ret = qp.run(cmd).getResponseCode();
            if (ret != 0) {
              qp.close();
              return ret;
            }

            ArrayList<String> res = new ArrayList<String>();

            printHeader(qp, out);

            int counter = 0;
            try {
              while (qp.getResults(res)) {
                for (String r : res) {
                  out.println(r);
                }
                counter += res.size();
                res.clear();
                if (out.checkError()) {
                  break;
                }
              }
            } catch (IOException e) {
              console.printError("Failed with exception " + e.getClass().getName() + ":"
                  + e.getMessage(), "\n"
                  + org.apache.hadoop.util.StringUtils.stringifyException(e));
              ret = 1;
            }

            int cret = qp.close();
            if (ret == 0) {
              ret = cret;
            }

            long end = System.currentTimeMillis();
            double timeTaken = (end - start) / 1000.0;
            console.printInfo("Time taken: " + timeTaken + " seconds" +
                (counter == 0 ? "" : ", Fetched: " + counter + " row(s)"));

          } else {
            String firstToken = tokenizeCmd(cmd.trim())[0];
            String cmd_1 = getFirstCmd(cmd.trim(), firstToken.length());

            if (ss.getIsVerbose()) {
              ss.out.println(firstToken + " " + cmd_1);
            }
            CommandProcessorResponse res = proc.run(cmd_1);
            if (res.getResponseCode() != 0) {
              ss.out.println("Query returned non-zero code: " + res.getResponseCode() +
                  ", cause: " + res.getErrorMessage());
            }
            ret = res.getResponseCode();
          }
        }
      } catch (CommandNeedRetryException e) {
        console.printInfo("Retry query with a different approach...");
        tryCount++;
        needRetry = true;
      }
    } while (needRetry);

    return ret;
  }

	public static void main(String[] args) throws Exception {
    int ret = new PantheraCliDriver().run(args);
    System.exit(ret);
  }
	
	/**
   * Execute the cli work
   * @param ss CliSessionState of the CLI driver
   * @param conf HiveConf for the driver sionssion
   * @param oproc Opetion processor of the CLI invocation
   * @return status of the CLI comman execution
   * @throws Exception
   */
  private  int executeDriver(CliSessionState ss, HiveConf conf, OptionsProcessor oproc)
      throws Exception {

    // connect to Hive Server
    if (ss.getHost() != null) {
      ss.connect();
      if (ss.isRemoteMode()) {
        prompt = "[" + ss.getHost() + ':' + ss.getPort() + "] " + prompt;
        char[] spaces = new char[prompt.length()];
        Arrays.fill(spaces, ' ');
        prompt2 = new String(spaces);
      }
    }

    // CLI remote mode is a thin client: only load auxJars in local mode
    if (!ss.isRemoteMode() && !ShimLoader.getHadoopShims().usesJobShell()) {
      // hadoop-20 and above - we need to augment classpath using hiveconf
      // components
      // see also: code in ExecDriver.java
      ClassLoader loader = conf.getClassLoader();
      String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
      if (StringUtils.isNotBlank(auxJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","));
      }
      conf.setClassLoader(loader);
      Thread.currentThread().setContextClassLoader(loader);
    }

    PantheraCliDriver cli = new PantheraCliDriver();
    cli.setHiveVariables(oproc.getHiveVariables());

    // use the specified database if specified
    cli.processSelectDatabase(ss);

    // Execute -i init files (always in silent mode)
    cli.processInitFiles(ss);

    if (ss.execString != null) {
      int cmdProcessStatus = cli.processLine(ss.execString);
      return cmdProcessStatus;
    }

    try {
      if (ss.fileName != null) {
        return cli.processFile(ss.fileName);
      }
    } catch (FileNotFoundException e) {
      System.err.println("Could not open input file for reading. (" + e.getMessage() + ")");
      return 3;
    }

    ConsoleReader reader =  getConsoleReader();
    reader.setBellEnabled(false);
    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
    for (Completor completor : getCommandCompletor()) {
      reader.addCompletor(completor);
    }

    String line;
    final String HISTORYFILE = ".hivehistory";
    String historyDirectory = System.getProperty("user.home");
    try {
      if ((new File(historyDirectory)).exists()) {
        String historyFile = historyDirectory + File.separator + HISTORYFILE;
        reader.setHistory(new History(new File(historyFile)));
      } else {
        System.err.println("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.");
      }
    } catch (Exception e) {
      System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
                         "history file.  History will not be available during this session.");
      System.err.println(e.getMessage());
    }

    int ret = 0;

    String prefix = "";
    String curDB = getFormattedDb(conf, ss);
    String curPrompt = prompt + curDB;
    String dbSpaces = spacesForString(curDB);

    while ((line = reader.readLine(curPrompt + "> ")) != null) {
      if (!prefix.equals("")) {
        prefix += '\n';
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line;
        ret = cli.processLine(line, true);
        prefix = "";
        curDB = getFormattedDb(conf, ss);
        curPrompt = prompt + curDB;
        dbSpaces = dbSpaces.length() == curDB.length() ? dbSpaces : spacesForString(curDB);
      } else {
        prefix = prefix + line;
        curPrompt = prompt2 + dbSpaces;
        continue;
      }
    }
    return ret;
  }
  
  /**
   * If enabled and applicable to this command, print the field headers
   * for the output.
   *
   * @param qp Driver that executed the command
   * @param out Printstream which to send output to
   */
  private void printHeader(Driver qp, PrintStream out) {
    List<FieldSchema> fieldSchemas = qp.getSchema().getFieldSchemas();
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)
          && fieldSchemas != null) {
      // Print the column names
      boolean first_col = true;
      for (FieldSchema fs : fieldSchemas) {
        if (!first_col) {
          out.print('\t');
        }
        out.print(fs.getName());
        first_col = false;
      }
      out.println();
    }
  }
  
  /**
   * Extract and clean up the first command in the input.
   */
  private String getFirstCmd(String cmd, int length) {
    return cmd.substring(length).trim();
  }

  private String[] tokenizeCmd(String cmd) {
    return cmd.split("\\s+");
  }
  
  /**
   * Retrieve the current database name string to display, based on the
   * configuration value.
   * @param conf storing whether or not to show current db
   * @param ss CliSessionState to query for db name
   * @return String to show user for current db value
   */
  private static String getFormattedDb(HiveConf conf, CliSessionState ss) {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIPRINTCURRENTDB)) {
      return "";
    }
    //BUG: This will not work in remote mode - HIVE-5153
    String currDb = SessionState.get().getCurrentDatabase();

    if (currDb == null) {
      return "";
    }

    return " (" + currDb + ")";
  }
  
  /**
   * Generate a string of whitespace the same length as the parameter
   *
   * @param s String for which to generate equivalent whitespace
   * @return  Whitespace
   */
  private static String spacesForString(String s) {
    if (s == null || s.length() == 0) {
      return "";
    }
    return String.format("%1$-" + s.length() +"s", "");
  }

  public void setHiveVariables(Map<String, String> hiveVariables) {
    SessionState.get().setHiveVariables(hiveVariables);
  }
}
