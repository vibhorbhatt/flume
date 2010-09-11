package com.cloudera.flume.handlers.avro;

@SuppressWarnings("all")
public class AvroFlumeEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AvroFlumeEvent\",\"namespace\":\"com.cloudera.flume.handlers.avro\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"priority\",\"type\":{\"type\":\"enum\",\"name\":\"Priority\",\"symbols\":[\"FATAL\",\"ERROR\",\"WARN\",\"INFO\",\"DEBUG\",\"TRACE\"]}},{\"name\":\"body\",\"type\":\"bytes\"},{\"name\":\"nanos\",\"type\":\"long\"},{\"name\":\"host\",\"type\":\"string\"},{\"name\":\"fields\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}}]}");
  public long timestamp;
  public com.cloudera.flume.handlers.avro.Priority priority;
  public java.nio.ByteBuffer body;
  public long nanos;
  public org.apache.avro.util.Utf8 host;
  public java.util.Map<org.apache.avro.util.Utf8,java.nio.ByteBuffer> fields;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return timestamp;
    case 1: return priority;
    case 2: return body;
    case 3: return nanos;
    case 4: return host;
    case 5: return fields;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: timestamp = (java.lang.Long)value$; break;
    case 1: priority = (com.cloudera.flume.handlers.avro.Priority)value$; break;
    case 2: body = (java.nio.ByteBuffer)value$; break;
    case 3: nanos = (java.lang.Long)value$; break;
    case 4: host = (org.apache.avro.util.Utf8)value$; break;
    case 5: fields = (java.util.Map<org.apache.avro.util.Utf8,java.nio.ByteBuffer>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
