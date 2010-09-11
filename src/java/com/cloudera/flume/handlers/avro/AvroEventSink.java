package com.cloudera.flume.handlers.avro;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Clock;

public class AvroEventSink extends EventSink.Base {

  static Logger LOG = Logger.getLogger(AvroEventSink.class);

  final public static String A_SERVERHOST = "serverHost";
  final public static String A_SERVERPORT = "serverPort";
  final public static String A_SENTBYTES = "sentBytes";

  protected FlumeEventAvroServer avroClient;
  String host;
  int port;
  HttpTransceiver transport;

  // this boolean variable is not used anywhere
  boolean nonblocking;
  /*
   * The following variables keeps track of the number of bytes of the
   * Event.body shipped.
   */
  AtomicLong sentBytes = new AtomicLong();

  public AvroEventSink(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public void append(Event e) throws IOException {
    // convert the flumeEvent to avroevent
    AvroFlumeEvent afe = AvroEventAdaptor.convert(e);
    // Make sure client side is initialized.
    this.ensureInitialized();
    try {
      avroClient.append(afe);
      sentBytes.addAndGet(e.getBody().length);
      super.append(e);
    } catch (AvroRemoteException e1) {
      throw new IOException("Append failed " + e1.getMessage(), e1);
    }
  }

  private void ensureInitialized() throws IOException {
    if (this.avroClient == null || this.transport == null) {
      throw new IOException("MasterRPC called while not connected to master");
    }
  }

  @Override
  public void open() throws IOException {

    URL url = new URL("http", host, port, "/");
    transport = new HttpTransceiver(url);
    try {
      this.avroClient = (FlumeEventAvroServer) SpecificRequestor.getClient(
          FlumeEventAvroServer.class, transport);
    } catch (Exception e) {
      throw new IOException("Failed to open Avro event sink at " + host + ":"
          + port + " : " + e.getMessage());
    }
    LOG.info("AvroEventSink open on port  " + port);
  }

  @Override
  public void close() throws IOException {
    if (transport != null) {
      transport.close();
      transport = null;
      LOG.info("AvrotEventSink on port " + port + " closed");
    }
  }

  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setStringMetric(A_SERVERHOST, host);
    rpt.setLongMetric(A_SERVERPORT, port);
    rpt.setLongMetric(A_SENTBYTES, sentBytes.get());
    return rpt;
  }

  /**
   * Just for testting.
   */
  public static void main(String argv[]) throws IOException {
    FlumeConfiguration conf = FlumeConfiguration.get();
    int port = conf.getCollectorPort();
    FlumeEventAvroServerImpl testServer = new FlumeEventAvroServerImpl(port);

    AvroEventSink sink = new AvroEventSink("localhost", port);
    
    
    try {
      testServer.start();
      sink.open();

      for (int i = 0; i < 100; i++) {
        Event e = new EventImpl(("This is a test " + i).getBytes(), Clock
            .unixTime(), Priority.INFO, Clock.nanos(), "host");

        e.set("pop", "pop".getBytes());
        sink.append(e);
        LOG.info("Test Message " + i + " shipped");
        Thread.sleep(200);

      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      testServer.close();
      sink.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * These methods can be deleted now that we have a wrapper classes
   * RpcSource/Sink with the builder into it. Left it for the deprecated
   * sources/sinks.
   */
  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        if (args.length > 2) {
          throw new IllegalArgumentException(
              "usage: avro([hostname, [portno]]) ");
        }
        String host = FlumeConfiguration.get().getCollectorHost();
        int port = FlumeConfiguration.get().getCollectorPort();
        if (args.length >= 1) {
          host = args[0];
        }

        if (args.length >= 2) {
          port = Integer.parseInt(args[1]);
        }
        return new AvroEventSink(host, port);
      }
    };

  }
}
