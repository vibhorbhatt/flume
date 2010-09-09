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

package com.cloudera.flume.handlers.avro;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TSaneThreadPoolServer;
import org.apache.thrift.transport.TSaneServerSocket;
import org.apache.thrift.transport.TTransportException;

import sun.tools.tree.ThisExpression;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This sets up the port that listens for incoming flume event rpc calls.
 */
public class AvroEventSource extends EventSource.Base {
  final static int DEFAULT_QUEUE_SIZE = FlumeConfiguration.get()
      .getThriftQueueSize();
  final static long MAX_CLOSE_SLEEP = FlumeConfiguration.get()
      .getThriftCloseMaxSleep();

  final static Logger LOG = Logger.getLogger(AvroEventSource.class);

  public static final String A_QUEUE_CAPACITY = "queueCapacity";
  public static final String A_QUEUE_FREE = "queueFree";
  public static final String A_ENQUEUED = "enqueued";
  public static final String A_DEQUEUED = "dequeued";
  public static final String A_BYTES_IN = "bytesIn";

  final int port;
  FlumeEventAvroServerImpl svr;
  //TSaneThreadPoolServer server;

  final BlockingQueue<Event> q;
  final AtomicLong enqueued = new AtomicLong();
  final AtomicLong dequeued = new AtomicLong();
  final AtomicLong bytesIn = new AtomicLong();

  boolean closed = true;

  /**
   * Create a thrift event source listening on port with a qsize buffer.
   */
  public AvroEventSource(int port, int qsize) {
    this.port = port;
    this.svr = new FlumeEventAvroServerImpl(port);
    this.q = new LinkedBlockingQueue<Event>(qsize);
  }

  /**
   * Get reportable data from the thrift event source.
   * 
   * @Override
   */
  synchronized public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setLongMetric(A_QUEUE_CAPACITY, q.size());
    rpt.setLongMetric(A_QUEUE_FREE, q.remainingCapacity());
    rpt.setLongMetric(A_ENQUEUED, enqueued.get());
    rpt.setLongMetric(A_DEQUEUED, dequeued.get());
    //rpt.setLongMetric(A_BYTES_IN, svr.getBytesReceived());
    return rpt;
  }

  /**
   * This constructor allows the for an arbitrary blocking queue implementation.
   */
  public AvroEventSource(int port, BlockingQueue<Event> q) {
    Preconditions.checkNotNull(q);
    this.port = port;
     this.q = q;
    
  }

  public AvroEventSource(int port) {
    this(port, DEFAULT_QUEUE_SIZE);
  }

  /**
   * Exposed for testing.
   */
  void enqueue(Event e) throws IOException {
    try {
      q.put(e);
      enqueued.getAndIncrement();
    } catch (InterruptedException e1) {
      LOG.error("blocked append was interrupted", e1);
      throw new IOException(e1);
    }
  }

  @Override
  synchronized public void open() throws IOException {
    
    this.svr= new FlumeEventAvroServerImpl(port){
      
      @Override
      public Void append(AvroFlumeEvent evt) throws AvroRemoteException{
        // convert AvroEvent evt -> e
       
        try {
          enqueue(AvroEventAdaptor.convert(evt));
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        
        super.append(evt);
        return null;
      
      }
    };
  }

  @Override
  synchronized public void close() throws IOException {
 
    long sz = q.size();
    LOG.info(String.format("Queue still has %d elements ...", sz));

    // drain the queue
    // TODO (jon) parameterize queue drain max sleep is one minute
    long maxSleep = MAX_CLOSE_SLEEP;
    long start = Clock.unixTime();
    while (q.peek() != null) {
      if (Clock.unixTime() - start > maxSleep) {
        if (sz == q.size()) {
          // no progress made, timeout and close it.
          LOG
              .warn("Close timed out due to no progress.  Closing despite having "
                  + q.size() + " values still enqued");
          return;
        }
        // there was some progress, go another cycle.
        start = Clock.unixTime();
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("Unexpected interrupt of close " + e.getMessage(), e);
      }
    }

    closed = true;
    return;
  }

  @Override
  public Event next() throws IOException {

    try {

      Event e = null;
      // block until an event shows up
      while ((e = q.poll(100, TimeUnit.MILLISECONDS)) == null) {

        synchronized (this) {
          // or bail out if closed
          if (closed) {
            return null;
          }
        }

      }
      // return the event
      synchronized (this) {
        dequeued.getAndIncrement();
        updateEventProcessingStats(e);
        return e;
      }
    } catch (InterruptedException e) {
      throw new IOException("Waiting for queue element was interupted! "
          + e.getMessage(), e);
    }
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length == 1, "usage: tSource(port)");

        int port = Integer.parseInt(argv[0]);

        return new AvroEventSource(port);
      }

    };
  }
}
