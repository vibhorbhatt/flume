/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.flume.master.commands;

import java.io.IOException;

import com.cloudera.flume.master.Command;
import com.cloudera.flume.master.Execable;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.master.MasterExecException;
import com.google.common.base.Preconditions;

/**
 * This implements the "settlimit" command
 */
public class SetLimitForm {

  String logicalNode;

  public String getLogicalNode() {
    return logicalNode;
  }

  public void setLogicalNode(String logicalNode) {
    this.logicalNode = logicalNode;
  }

  /**
   * Convert this bean into a command.
   */
  public Command toCommand() {
    String[] args = { "physicalnode", "chokeID", "limit" };
    return new Command("settlimit", args);
  }

  /**
   * Build an execable that will execute the command.
   */
  public static Execable buildExecable() {
    return new Execable() {
      @Override
      public void exec(String[] args) throws MasterExecException, IOException {
        //first check the length of the arguments
        if (args.length <2) {
          throw new MasterExecException("missing arguments", null);
             }
        
        String physicalNodeName = args[0];
        // one should check whether the physicalnodename even exists
        if (FlumeMaster.getInstance().getSpecMan().getLogicalNode(
            physicalNodeName).isEmpty()) {
          throw new MasterExecException("PhysicalNode Does Not Exist", null);
             }
        
        String chokerName = "";
        int limit=Integer.MAX_VALUE;
        
        if (args.length == 2) {
          //This is when only the physicalNode limit is passed
          limit = Integer.parseInt(args[1]);

        } else if (args.length>2){
          chokerName = args[1];
          limit = Integer.parseInt(args[2]);
        }
        // only works in memory!! not in zookeeper.
        FlumeMaster.getInstance().getSpecMan().addChokeLimit(physicalNodeName,
            chokerName, limit);
      }
    };
  }
}
