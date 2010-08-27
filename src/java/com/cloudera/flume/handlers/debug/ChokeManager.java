package com.cloudera.flume.handlers.debug;

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

  // Time quanta in millisecs. It is a
  // constant right now, we can change
  // this later. The main thread of the Chokemanager fills up the buckets
  // correcponding to different choke-ids and the physical node after this time
  // quanta.

  public static final int timeQuanta = 100;

  // maximum number of bytes allowed to be sent in the time quanta through a
  // physicalNode.
  private int physicalLimit;

  // this tells whether the ChokeManager is active or not
  private volatile boolean active = false;
  private final int payLoadheadrsize = 50;
  public final HashMap<String, ThrottleInfoData> idtoThrottleInfoMap = new HashMap<String, ThrottleInfoData>();

  // this is the reader-writer lock on the idtoThrottleInfoMap. Whever it is
  // being updated, a writelock has to be taken on it, and when someone is just
  // reading the map, readlock on it is sufficient.

  ReentrantReadWriteLock rwl_idtoThrottleInfoMap;

  public ChokeManager(int limit) {
    super();
    // PhysicalNodeLimit is set right in the constructor

    rwl_idtoThrottleInfoMap = new ReentrantReadWriteLock();
    this.physicalLimit = limit * ChokeManager.timeQuanta;

    // register the universal Id.
    register("U", 0);

    init();
  }

  // create a fake initiator; one can initiate bunch of different ids with their
  // different limits initially.
  private void init() {
    rwl_idtoThrottleInfoMap.writeLock().lock();
    register("a", 500);
    register("b", 800);
    register("c", 1000);

    rwl_idtoThrottleInfoMap.writeLock().unlock();
  }

  /**
   * This method is the only method used to add entries to the
   * idtoThrottleInfoMap.
   */
  public synchronized void register(String throttleID, int limit) {

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
  public synchronized void updateidtoThrottleInfoMap(
      HashMap<String, Integer> newMap) {

    rwl_idtoThrottleInfoMap.writeLock().lock();

    for (String s : newMap.keySet()) {
      register(s, newMap.get(s));
    }
    // Set the PhysicalNode limit, which corresponds to entry for the ""
    // string.
    // First make sure that there is an entry for the empty key.
    if (newMap.containsKey("")) {
      // ideally this should always true
      this.physicalLimit = newMap.get("");
    }
    rwl_idtoThrottleInfoMap.writeLock().unlock();
  }

  public boolean isChokeId(String ID) {
    Boolean res;
    rwl_idtoThrottleInfoMap.readLock().lock();
    res = this.idtoThrottleInfoMap.containsKey(ID);

    rwl_idtoThrottleInfoMap.readLock().unlock();
    return res;
  }

  @Override
  public void run() {

  
    //Version version = RuntimeVersion.VERSION;
    
   // System.out.println("Hey I am in the Cmanager with Java version :"+version + " "+System.getProperty("java.version")+" !!\n");
    
    
    active = true;

    while (this.active) {
      try {
        Thread.sleep(timeQuanta);
      } catch (InterruptedException e) {
      }

      // System.out.println("Hey I am in the run method of Cmanager 1!!\n");

      rwl_idtoThrottleInfoMap.readLock().lock();
      // the main policy logic comes here

      // System.out.println("Hey I am in Cmanager Run, Dercrs = " +
      // FlumeNode.getInstance().getChokeManager().idtoThrottleInfoMap.size()+
      // "\n");
      for (String key : this.idtoThrottleInfoMap.keySet()) {
        synchronized (this.idtoThrottleInfoMap.get(key)) {
          // this.idtoThrottleInfoMap.get(key).printState();
          this.idtoThrottleInfoMap.get(key).bucketFillup();
          this.idtoThrottleInfoMap.get(key).notifyAll();
        }
      }
      rwl_idtoThrottleInfoMap.readLock().unlock();
    }
  }

  public void halt() {
    active = false;
  }

  /**
   * This is the method a choke-decorator calls inside its append. This method
   * ensures that only the allowed number of bytes are shipped in a certain time
   * quanta.  Also note that this method can block for a while but not forever.
   */
  public void deleteItems(String id, int numBytes) {

    rwl_idtoThrottleInfoMap.readLock().lock();
    // simple policy for now: if the chokeid is not there then simply return,
    // essentially no throttling.
    if (this.isChokeId(id) == false) {
      rwl_idtoThrottleInfoMap.readLock().unlock();
      return;
    }

    int loopCount = 0;
    synchronized (this.idtoThrottleInfoMap.get(id)) {
      while (this.active
          && !this.idtoThrottleInfoMap.get(id).bucketCompare(
              numBytes + this.payLoadheadrsize)) {

        // System.out.println("Hey I am blocked!!\n");

        try {
          this.idtoThrottleInfoMap.get(id).wait(ChokeManager.timeQuanta);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
        }

        if (loopCount++ >= 2) // just wait twice to avoid starvation
          break;
      }
      this.idtoThrottleInfoMap.get(id).removeTokens(
          numBytes + this.payLoadheadrsize);
      // We are not taking the physical limit into account, that's policy stuff
      // and will figure this out later
    }
    rwl_idtoThrottleInfoMap.readLock().unlock();
  }

}
