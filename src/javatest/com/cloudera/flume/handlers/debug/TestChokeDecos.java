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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;

/**
 * Demonstrates basic throttling works within some error-tolerance. There are
 * two different tests we perform here: IndividualChoke test and CollectiveChoke
 * test, their details are given with their respective Ttest methods below.
 */
public class TestChokeDecos {
  final public static Logger LOG = Logger.getLogger(TestChokeDecos.class);

  // payLoadheadesize should equal to one in ChokeManager
  private final int payLoadheadrsize = 50;
  Random generator = new Random(System.currentTimeMillis());

  final ChokeManager testChokeMan = new ChokeManager();
  final HashMap<String, Integer> chokeMap = new HashMap<String, Integer>();

  final long testTime = 5000; // in milisecs

  // number of drivers created for the testing
  final int numFakeDrivers = 50;

  // here we set the limits for the minimum and maximum throttle rates in KB/sec
  int minTlimit = 500;
  int maxTlimit = 20000;
  // here we set the limits on size (in Bytes) of the messages passed
  int minMsgSize = 50;
  int maxMsgSize = 30000;

  // Error tolerance constants, these ratios are the max and min limits set on
  // the following quantity: MaxBytes allowed/Bytes Actually shipped.
  double highErrorLimit = 1.5;
  double lowErrorLimit = .8;

  /**
   * This extends the ChokeDecorator with the added functionality of
   * book-keeping the number of bytes shipped through it.
   */
  class TestChoke<S extends EventSink> extends ChokeDecorator<S> {
    private long numBytesShipped = 0;
    private final ChokeManager chokeMan;

    public TestChoke(S s, String chokeId, ChokeManager cman) {
      super(s, chokeId);
      this.chokeMan = cman;
    }

    /**
     * We are overriding this because the method in ChokeManager calls
     * super.append() and we want to avoid this as the higher-level sink is not
     * initialized. In this method we just eliminate that call.
     */
    @Override
    public void append(Event e) throws IOException {
      try {
        chokeMan.deleteItems(chokeId, e.getBody().length);
      } catch (Exception e1) {
        throw new IOException(e1.getMessage(), e1);
      }
    }

    public synchronized void updateBytesCount(int numBytes) {
      // add the payload headersize beause the same is added in the deleteitems
      // of the ChokeManager
      numBytesShipped += numBytes + payLoadheadrsize;
    }

    /**
     *This method returns the total number of bytes shipped accross the choke
     * decorator.
     */
    public synchronized long getByteCount() {
      return numBytesShipped;
    }
  }

  /**
   * This class is used for emulating a DirectDriver. This essentially runs the
   * continous loop of generating a random length string and calling an append
   * on a choke 'myChoke' where 'myChoke' is passed to this driver through the
   * constructor.
   * 
   */
  class FakeDriver extends Thread {
    TestChoke<EventSink> myChoke;
    private volatile Boolean active = false;

    public FakeDriver(TestChoke testChoke) {
      this.myChoke = testChoke;
    }

    /*
     * This is simulating the main loop of the DirectDriver thread.
     */
    @Override
    public void run() {
      active = true;
      String randomStr = "A";

      int i = 0, loopcount = 0;
      EventImpl eI = new EventImpl("".getBytes());
      // We will create the random length string here. As the contents of the
      // string don't matter it simply is a random length string of 'A's. The
      // length of the string is in the range [minMsgSize, maxMsgSize].
      for (i = 0; i < minMsgSize + generator.nextInt(maxMsgSize - minMsgSize); i++) {
        randomStr += "A";
      }
      eI = new EventImpl(randomStr.getBytes());

      while (active) {
        // From time to time we change the length of the string. If we do it
        // very frequently then it throws us off from the target,
        // we don't produce the data at speeds close to the max-limit.
        if ((++loopcount) % 1000 == 999) {
          randomStr = "";
          for (i = 0; i < minMsgSize
              + generator.nextInt(maxMsgSize - minMsgSize); i++) {
            randomStr += "A";
          }
          eI = new EventImpl(randomStr.getBytes());
        }
        // now we call the append on the choke
        try {
          myChoke.append(eI);
        } catch (IOException e) {
          LOG.error("Exception thrown: " + e.getMessage());
        }
        // update the bytecount on this ChokeId
        myChoke.updateBytesCount(eI.getBody().length);
      }
    }

    public void halt() {
      active = false;
    }
  }

  /**
   * The high level goal of this test is to see if many drivers using different
   * chokes can ship the data approximately at the max-limit set on them. In
   * more detail, in this test we create a bunch of FakeDrivers, and for each of
   * these drivers we assign them a unique choke. Then we run these Drivers and
   * check if the amount of data shipped accross each choke is approximately
   * what we expect.
   */
  @Test
  public void runInvidualChokeTest() throws InterruptedException {
    // number of chokes is equal to the number of drivers
    int numChokes = numFakeDrivers;
    LOG.info("Setting up Individual Test");
    // create some chokeIDs with random limit in the range specified
    for (int i = 0; i < numChokes; i++) {
      // different chokesIds are created with their ids coming from the range
      // "1", "2", "3"...
      // with a throttlelimit in the range [minTlimit, maxTlimit]
      chokeMap.put(Integer.toString(i), minTlimit
          + generator.nextInt(maxTlimit - minTlimit));
    }
    // update the chokemap with these chokes
    testChokeMan.updateidtoThrottleInfoMap(chokeMap);
    // now we create bunch of chokes
    TestChoke[] tchokeArray = new TestChoke[numChokes];
    for (int i = 0; i < numChokes; i++) {
      // different chokes are created with their ids coming from the range "0",
      // "1", "2", "3"..."numChokes"
      tchokeArray[i] = new TestChoke<EventSink>(null, Integer.toString(i),
          testChokeMan);
    }
    // one driver for each choke
    FakeDriver[] fakeDriverArray = new FakeDriver[numFakeDrivers];
    for (int i = 0; i < numFakeDrivers; i++) {
      // Driver i is mapped to ith choke, simple 1 to 1 mapping.
      fakeDriverArray[i] = new FakeDriver(tchokeArray[i]);
    }

    // check if all the ChokeIDs are present in the chokeMap
    LOG.info("Running the Individual Test Now!");
    for (int i = 0; i < numFakeDrivers; i++) {
      if (!testChokeMan.isChokeId(Integer.toString(i))) {
        LOG.error("ChokeID " + Integer.toString(i) + "not present");
        fail();
      }
    }
    // Now we start the test.
    // Start the ChokeManager.
    testChokeMan.start();
    for (FakeDriver f : fakeDriverArray) {
      f.start();
    }
    // stop for the allotted time period
    Thread.sleep(testTime);

    // Stop everything!
    for (FakeDriver f : fakeDriverArray) {
      f.halt();
    }
    testChokeMan.halt();
    // Take a little breather
    // Thread.sleep(100);

    // Now do the error evaluation, see how many bits were actually shipped.
    double errorRatio = 1.0;

    for (TestChoke<EventSink> t : tchokeArray) {
      // Now we compute the error ratio: Max/Actual.
      // Where Max= Maximum bytes which should have been shipped based on the
      // limit on this choke, and actual= bytes that were actually shipped.
      errorRatio = ((double) (chokeMap.get(t.getChokeId()) * testTime))
          / (double) (t.getByteCount());

      LOG.info("ChokeID: " + t.getChokeId() + ", error-ratio: " + errorRatio);
      // Test if the error ratio is in the limit we want.
      assertFalse((errorRatio > this.highErrorLimit || errorRatio < this.lowErrorLimit));
    }
    LOG.info("Individual Test successful  !!!");
  }

  /**
   * The high level goal of this test is to make many driver threads contend
   * together on same chokes and see if they collectively ship the bytes under
   * the limits we want. We create just few chokes here, and for each Driver we
   * assign them one of these chokes at random.
   */

  @Test
  public void runCollectiveChokeTest() throws InterruptedException {
    // Few Chokes
    int numChokes = 5;

    LOG.info("Setting up Collective Test");

    // create chokeIDs with random limit range
    for (int i = 0; i < numChokes; i++) {
      // different chokesIds are created with their ids coming from the range
      // "0", "1", "2", "3"...
      // with a throttlelimit in the range [minTlimit, maxTlimit]
      chokeMap.put(Integer.toString(i), minTlimit
          + generator.nextInt(maxTlimit - minTlimit));
    }

    // update the chokemap with these chokes
    testChokeMan.updateidtoThrottleInfoMap(chokeMap);
    // Initialize the chokes appropriately.
    TestChoke[] tchokeArray = new TestChoke[numChokes];
    for (int i = 0; i < numChokes; i++) {
      // different chokes are created with their ids coming from the range "0",
      // "1", "2", "3"..."numFakeDrivers"
      tchokeArray[i] = new TestChoke<EventSink>(null, Integer.toString(i),
          testChokeMan);
    }

    // As we are assigning the chokes to drivers at random, there is a chance
    // that not all initialized chokes are assigned to some driver. So the
    // number of bytes shipped on these chokes will be zero, which will throw us
    // off in the error evaluation. For this reason we add all the chokes
    // assigned tosome driver in a set, and do error evaluation only on those
    // chokes.

    // chokesUsed is the set of chokes assigned to some driver.
    Set<TestChoke<EventSink>> chokesUsed = new HashSet<TestChoke<EventSink>>();

    FakeDriver[] fakeDriverArray = new FakeDriver[numFakeDrivers];
    // Each dirver is randomly assigned to a random choke in the range
    // [0,numChokes)
    int randChokeIndex = 0;
    for (int i = 0; i < numFakeDrivers; i++) {
      randChokeIndex = generator.nextInt(numChokes);
      fakeDriverArray[i] = new FakeDriver(tchokeArray[randChokeIndex]);
      // adds this choke to the set of chokesUsed
      chokesUsed.add(tchokeArray[randChokeIndex]);
    }

    // check if all the ChokeIDs are present
    LOG.info("Running the Collective Test Now!");
    for (TestChoke<EventSink> t : chokesUsed) {
      if (!testChokeMan.isChokeId(t.getChokeId())) {
        LOG.error("ChokeID " + t.getChokeId() + "not present");
        fail();
      }
    }

    // Now we start the test.
    // start the ChokeManager
    testChokeMan.start();
    for (FakeDriver f : fakeDriverArray) {
      f.start();
    }
    Thread.sleep(testTime);

    // Stop everything!
    for (FakeDriver f : fakeDriverArray) {
      f.halt();
    }
    testChokeMan.halt();
    // Take a little breather
    // Thread.sleep(100);
    // now do the error evaluation
    double errorRatio = 1.0;

    for (TestChoke<EventSink> t : chokesUsed) {
      // Now we compute the error ratio: Max/Actual.
      // Where Max= Maximum bytes which should have been shipped based on the
      // limit on this choke, and actual= bytes that were actually shipped.
      errorRatio = ((double) (chokeMap.get(t.getChokeId()) * testTime))
          / (double) (t.getByteCount());

      LOG.info("ChokeID: " + t.getChokeId() + ", error-ratio: " + errorRatio);
      // Test if the error ratio is in the limit we want.
      assertFalse((errorRatio > this.highErrorLimit || errorRatio < this.lowErrorLimit));
    }
    LOG.info("Collective test successful  !!!");
  }
}
