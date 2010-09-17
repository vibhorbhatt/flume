package com.cloudera.flume.handlers.avro;

import java.io.IOException;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.specific.SpecificResponder;

public class FlumeEventAvroServerImpl implements FlumeEventAvroServer {
  private HttpServer http;
  private final int port;
/**
 *  This just sets the port for this AvroServer 
 */
  public FlumeEventAvroServerImpl(int port) {
    this.port = port;
  }

  /**
   * This blocks till the server starts.
   */
  public void start() throws IOException {
    SpecificResponder res = new SpecificResponder(FlumeEventAvroServer.class,
        this);
    this.http = new HttpServer(res, port);
    this.http.start();
    // Current version of Avro 1.3.3 block the call below.
  }

  @Override
  public void append(AvroFlumeEvent evt)  {
  }

/**
 * Stops the FlumeEventAvroServer, called only from the server. 
 */
  public Void close() throws AvroRemoteException {
    http.close();
    return null;
  }

}
