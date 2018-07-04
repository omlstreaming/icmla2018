package org.omlstreaming.spark;

import java.sql.Timestamp;

public class StdClass {

  private long ts, time1;
  private Timestamp timestamp;
  private double x, y, z;

  public StdClass(long ts, Timestamp timestamp, double x, double y, double z, long time1) {
    this.ts = ts;
    this.timestamp = timestamp;
    this.x = x;
    this.y = y;
    this.z = z;
    this.time1 = time1;
  }

  public long getTs() {
    return ts;
  }

  public void setTs(long ts) {
    this.ts = ts;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public double getX() {
    return x;
  }

  public void setX(double x) {
    this.x = x;
  }

  public double getY() {
    return y;
  }

  public void setY(double y) {
    this.y = y;
  }

  public double getZ() {
    return z;
  }

  public void setZ(double z) {
    this.z = z;
  }

  public long getTime1() {
    return time1;
  }

  public void setTime1(long time1) {
    this.time1 = time1;
  }
}
