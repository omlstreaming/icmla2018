package org.omlstreaming.performance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*

SAMPLE USAGE

Profiler.getInstance().start("func()");
// func() { }
Profiler.getInstance().stop("func()");

// get stats
System.out.println(Profiler.getInstance().toString());

// get certain value of interest "field" for probe "name"
GlobalFeatureProfiler.getInstance().toVal(name).get(field);

 */

public class GlobalFeatureProfiler {

  private static GlobalFeatureProfiler singletonInstance = null;

  private Map<String, Probe> probes;
  private List<Probe> probesStack;

  public static GlobalFeatureProfiler getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new GlobalFeatureProfiler();
    }
    return singletonInstance;
  }

  protected GlobalFeatureProfiler() {
    probes = new HashMap<String, Probe>();
    probesStack = new ArrayList<Probe>();
  }

  public void start(String name) {
    Probe p = probes.get(name);
    if (p == null) {
      p = new Probe(name);
      probes.put(name, p);
      probesStack.add(p);
    }
    p.start();
  }

  public void stop(String name) {
    Probe p = probes.get(name);
    if (p == null) {
      throw new RuntimeException("The probe " + name + " has not been created.");
    }
    p.stop();
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    for (int i = 0; i < probesStack.size(); i++) {
      sb.append(probesStack.get(i).toString());
    }
    return sb.toString();
  }

  public List<Long> toVal(String name){
    return probes.get(name).getValueStats();
  }

  private static class Probe {

    private static final String LOG_FORMAT = "%s,%d,%d,%d,%d,%d,%d,%d\n";

    private String name;
    private long startTime;
    private long callCount;
    private long instantTime;
    private long totalTime;
    private long minTime;
    private long maxTime;

    public Probe(String name) {
      this.name = name;
      this.callCount = 0;
      this.totalTime = 0;
      this.instantTime = 0;
      this.startTime = 0;
      this.minTime = Long.MAX_VALUE;
      this.maxTime = Long.MIN_VALUE;
    }

    public void start() {
      startTime = System.currentTimeMillis();
    }

    public void stop() {
      final long elapsed = (System.currentTimeMillis() - startTime);
      if (elapsed < minTime){
        minTime = elapsed;
      }
      if (elapsed > maxTime){
        maxTime = elapsed;
      }
      totalTime += elapsed;
      instantTime = elapsed;
      callCount++;
    }

    private String getFormattedStats(String format) {
      final long avgTime = callCount == 0 ? 0 : totalTime / callCount;
      final long delta = maxTime - minTime;
      return String
          .format(format, name, callCount, totalTime, instantTime, avgTime, minTime, maxTime, delta);
    }

    @Override
    public String toString() {
      return getFormattedStats(LOG_FORMAT);
    }

    private List<Long> getValueStats(){
      List<Long> retVal = new ArrayList<>();
      final long avgTime = callCount == 0 ? 0 : totalTime / callCount;
      final long delta = maxTime - minTime;

      retVal.add(callCount); retVal.add(totalTime);
      retVal.add(instantTime); retVal.add(avgTime);
      retVal.add(minTime); retVal.add(maxTime);
      retVal.add(delta);

      return retVal;
    }
  }
}