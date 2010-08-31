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

/**
 * This class contains the basic data elements required to throttle a ChokeDecorators.  
 * Essentially one single object is created for for every valid chokeID.
 */
package com.cloudera.flume.handlers.debug;

public class ThrottleInfoData {
  private int max = 1000;
  private int count = 0;
  private int bucket = 0;
  private String chokeID;

  public ThrottleInfoData(int limit, String id) {
    max = limit * ChokeManager.timeQuanta;
    count = bucket = 0;
    chokeID = id;
  }

  synchronized public void setMaxLimit(int limit) {
    max = limit * ChokeManager.timeQuanta;
  }

  synchronized public void removeTokens(int numTokens) {
    bucket = bucket - numTokens;
  }

  synchronized public Boolean bucketCompare(int numTokens) {
    return (bucket >= numTokens);
  }

  synchronized public void printState() {
    System.out.println("Choke Information of " + chokeID + ": Max =" + max
        + " Bucket=" + bucket + "\n");
  }

  synchronized public void bucketFillup() {
    bucket = max;
    count = 0;
  }

}
