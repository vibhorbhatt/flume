package com.cloudera.flume.handlers.debug;

public class ThrottleInfoData {
  private int max = 1000;
  private int count = 0;
  private int bucket = 0;
  private String chokeID;

 public ThrottleInfoData(int limit, String id) {
    max = limit*ChokeManager.timeQuanta;
    count = bucket = 0;
    chokeID=id;
  }

 synchronized public void setMaxLimit(int limit) {
    max = limit*ChokeManager.timeQuanta;
  }

 synchronized public void removeTokens(int numTokens) {
    bucket = bucket - numTokens;
  }

 synchronized public Boolean bucketCompare(int numTokens) {
    return (bucket >= numTokens);
  }

 synchronized public void printState() {
    System.out.println("Choke Information of " + chokeID+ ": Max =" + max + " Bucket=" + bucket + "\n");
  }
  
 synchronized public void bucketFillup() {
    bucket=max;
    count =0;
  }
  
}
