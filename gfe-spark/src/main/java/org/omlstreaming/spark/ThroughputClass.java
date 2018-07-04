package org.omlstreaming.spark;

import java.sql.Timestamp;

public class ThroughputClass {

  private Timestamp start;
  private Double avg;
  private Double variance;
  private Double skew;
  private Timestamp end;

  public ThroughputClass(Timestamp start, Double avg, Double variance, Double skew, Timestamp end) {
    this.start = start;
    this.avg = avg;
    this.variance = variance;
    this.skew = skew;
    this.end = end;
  }

  public Timestamp getStart() {
    return start;
  }

  public void setStart(Timestamp start) {
    this.start = start;
  }

  public Double getAvg() {
    return avg;
  }

  public void setAvg(Double avg) {
    this.avg = avg;
  }

  public Double getVariance() {
    return variance;
  }

  public void setVariance(Double variance) {
    this.variance = variance;
  }

  public Double getSkew() {
    return skew;
  }

  public void setSkew(Double skew) {
    this.skew = skew;
  }

  public Timestamp getEnd() {
    return end;
  }

  public void setEnd(Timestamp end) {
    this.end = end;
  }

}
