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
package com.cloudera.flume.util;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.specific.SpecificRequestor;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.avro.AvroFlumeConfigData;
import com.cloudera.flume.conf.avro.FlumeMasterAdminServerAvro;
import com.cloudera.flume.conf.avro.FlumeNodeStatusAvro;
import com.cloudera.flume.master.Command;
import com.cloudera.flume.master.MasterAdminServerAvro;
import com.cloudera.flume.master.MasterClientServerAvro;
import com.cloudera.flume.master.StatusManager.NodeStatus;

/**
 * Avro implementation of the Flume admin control RPC. This class manages the
 * connection to a master and provides type conversion.
 */
public class AdminRPCAvro implements AdminRPC {
  final static Logger LOG = Logger.getLogger(AdminRPCAvro.class);

  private String masterHostname;
  private int masterPort;
  private Transceiver trans;
  protected FlumeMasterAdminServerAvro masterClient;

  public AdminRPCAvro(String masterHost, int masterPort) throws IOException {
    this.masterHostname = masterHost;
    this.masterPort = masterPort;
    URL url = new URL("http", masterHostname, this.masterPort, "/");
    trans = new HttpTransceiver(url);
    this.masterClient = (FlumeMasterAdminServerAvro) SpecificRequestor
        .getClient(FlumeMasterAdminServerAvro.class, trans);
    LOG.info("Connected to master at " + masterHostname + ":" + masterPort);
  }

  @Override
  public Map<String, FlumeConfigData> getConfigs() throws IOException {
    Map<CharSequence, AvroFlumeConfigData> results = this.masterClient
        .getConfigs();
    Map<String, FlumeConfigData> out = new HashMap<String, FlumeConfigData>();
    for (CharSequence key : results.keySet()) {
      out.put(key.toString(), MasterClientServerAvro.configFromAvro(results
          .get(key)));
    }
    return out;
  }

  @Override
  public Map<String, NodeStatus> getNodeStatuses() throws IOException {
    Map<CharSequence, FlumeNodeStatusAvro> results = this.masterClient
        .getNodeStatuses();
    Map<String, NodeStatus> out = new HashMap<String, NodeStatus>();
    for (CharSequence key : results.keySet()) {
      out.put(key.toString(), MasterAdminServerAvro.statusFromAvro(results
          .get(key)));
    }
    return out;
  }

  @Override
  public Map<String, List<String>> getMappings(String physicalNode)
      throws IOException {
    Map<String, List<String>> mappings;

    mappings = new HashMap<String, List<String>>();

    for (Entry<CharSequence, List<CharSequence>> entry : masterClient
        .getMappings(physicalNode).entrySet()) {
      List<String> values;

      values = new LinkedList<String>();

      for (CharSequence cs : entry.getValue()) {
        values.add(cs.toString());
      }

      mappings.put(entry.getKey().toString(), values);
    }

    return mappings;
  }

  @Override
  public boolean hasCmdId(long cmdid) throws IOException {
    return this.masterClient.hasCmdId(cmdid);
  }

  @Override
  public boolean isFailure(long cmdid) throws IOException {
    return this.masterClient.isFailure(cmdid);
  }

  @Override
  public boolean isSuccess(long cmdid) throws IOException {
    return this.masterClient.isSuccess(cmdid);
  }

  @Override
  public long submit(Command command) throws IOException {
    return this.masterClient.submit(MasterAdminServerAvro
        .commandToAvro(command));
  }
}
