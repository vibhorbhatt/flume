package com.cloudera.flume.handlers.avro;

import java.io.IOException;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.specific.SpecificResponder;

public class FlumeEventAvroServerImpl implements FlumeEventAvroServer {
  HttpServer http;
  final int port;

  public FlumeEventAvroServerImpl(int port) {
    this.port = port;
  }

  public void start() throws IOException {
    SpecificResponder res = new SpecificResponder(FlumeEventAvroServer.class,
        this);
    http = new HttpServer(res, port);
  }

  @Override
  public Void append(AvroFlumeEvent evt) throws AvroRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Void close() throws AvroRemoteException {
    http.close();
    return null;
  }

}
