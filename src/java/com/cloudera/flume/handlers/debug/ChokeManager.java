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
package com.cloudera.flume.handlers.debug;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.sun.xml.internal.ws.util.RuntimeVersion;
import com.sun.xml.internal.ws.util.Version;

/**
 * Main throttling Logic is here. All the choke-decorators have to call a method
 * (deleteItems) of this class before calling the apend of their super class.
 * And in that method a thread is blocked if the Throttling limit either at the
 * PhysicalNode level or the choke-id level is reached. In this version, I've
 * ignored the physicalNode limit.
 */
public class ChokeManager extends Thread {

  // Time quanta in millisecs. It is a constant right now, we can change this
  // later. The main thread of the Chokemanager fills up the buckets
  // correcponding to different choke-ids and the physical node after this time
  // quanta.

  public static final int timeQuanta = 100;

  // maximum number of bytes allowed to be sent in the time quanta through a
  // physicalNode.
  // In this current version this is not taken into the account yet.
  private int physicalLimit;

  // this tells whether the ChokeManager is active or not
  private volatile boolean active = false;
  private int payLoadheadrsize = 50;
  private final HashMap<String, ThrottleInfoData> idtoThrottleInfoMap = new HashMap<String, ThrottleInfoData>();

  // This is the reader-writer lock on the idtoThrottleInfoMap. Whever it is
  // being updated, a writelock has to be taken on it, and when someone is just
  // reading the map, readlock on it is sufficient.

  ReentrantReadWriteLock rwl_idtoThrottleInfoMap;

  public ChokeManager() {
    super("ChokeManager");
    rwl_idtoThrottleInfoMap = new ReentrantReadWriteLock();
    this.physicalLimit = Integer.MAX_VALUE;
  }

  /**
   * this method is used to change the size of setPayLoadHeaderSize.
   */
  public void setPayLoadHeaderSize(int size) throws IllegalArgumentException {
    if (size < 0) {
      throw new IllegalArgumentException("PayloadHeaderSize cannot be negative");
    }
    this.payLoadheadrsize = size;
  }

  /**
   * This method is the only method used to add entries to the chokeMap
   * (idtoThrottleInfoMap). This method also assumes that a write-lock has been
   * already obtained on the Reader-Writer lock on ChokeMap
   * (rwl_idtoThrottleInfoMap).
   */
  private void register(String throttleID, int limit) {

    if (idtoThrottleInfoMap.get(throttleID) == null) {
      idtoThrottleInfoMap.put(throttleID, new ThrottleInfoData(limit,
          throttleID));
    }
    // set a new limit if the ID was already in use
    else {
      this.idtoThrottleInfoMap.get(throttleID).setMaxLimit(limit);
    }
  }

  /**
   * This method is called in the CheckLogicalNodes method of the livesness
   * manager. It gets the choke-d to limit mapping from the master and loads it
   * to idtoThrottleInfoMap.
   */
  public void updateIdtoThrottleInfoMap(HashMap<String, Integer> newMap) {

    rwl_idtoThrottleInfoMap.writeLock().lock();
    try {
      for (String s : newMap.keySet()) {
        register(s, newMap.get(s));
      }
      // Set the PhysicalNode limit, which corresponds to entry for the ""
      // string.

      // First make sure that there is an entry for the empty key which
      // corresponds to the physicalNode limit.
      if (newMap.containsKey("")) {
        // ideally this should always true
        this.physicalLimit = newMap.get("");
      }
    } finally {
      rwl_idtoThrottleInfoMap.writeLock().unlock();
    }

  }

  /**
   * This function returns true if the ChokeId passed is registered in the
   * ChokeManager.
   */
  public boolean isChokeId(String ID) {
    Boolean res;
    rwl_idtoThrottleInfoMap.readLock().lock();
    try {
      res = this.idtoThrottleInfoMap.containsKey(ID);
    } finally {
      rwl_idtoThrottleInfoMap.readLock().unlock();
    }
    return res;
  }

  @Override
  public void run() {
    active = true;

    while (this.active) {
      try {
        Thread.sleep(timeQuanta);
      } catch (InterruptedException e) {
        /*
         * Essentially send the control back to the beginning of the while loop.
         * If the ChokeManager is Halted(), this.active would be false and we
         * would fall out of this method.
         */
        continue;
      }

      rwl_idtoThrottleInfoMap.readLock().lock();
      // the main policy logic comes here
      try {
        for (ThrottleInfoData choke : this.idtoThrottleInfoMap.values()) {
          synchronized (choke) {
            // choke.printState();
            choke.bucketFillup();
            choke.notifyAll();
          }
        }
      } finally {
        rwl_idtoThrottleInfoMap.readLock().unlock();
      }
    }
  }

  public void halt() {
    active = false;
  }

  /**
   * This is the method a choke-decorator calls inside its append. This method
   * ensures that only the allowed number of bytes are shipped in a certain time
   * quanta. Also note that this method can block for a while but not forever.
   * This method blocks atmost for 2 time quantas. So if many driver threads are
   * using the same choke and the message size is huge, accuracy can be thrown
   * off, i.e., more bytes than the maximum limit can be shipped.
   */
  public void spendTokens(String id, int numBytes) throws IOException {
    rwl_idtoThrottleInfoMap.readLock().lock();
    try {
      // simple policy for now: if the chokeid is not there then simply return,
      // essentially no throttling with an invalid chokeID.
      if (this.isChokeId(id) != false) {
        int loopCount = 0;
        ThrottleInfoData myTinfoData = this.idtoThrottleInfoMap.get(id);
        synchronized (myTinfoData) {
          while (this.active
              && !myTinfoData.bucketCompare(numBytes + this.payLoadheadrsize)) {
            try {
              myTinfoData.wait(ChokeManager.timeQuanta);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException(e);
            }
            if (loopCount++ >= 2) // just wait twice to avoid starvation
              break;
          }
          myTinfoData.removeTokens(numBytes + this.payLoadheadrsize);
          // We are not taking the physical limit into account, that's policy
          // stuff and we'll figure this out later
        }
      }
    } finally {
      rwl_idtoThrottleInfoMap.readLock().unlock();
    }
  }
}
