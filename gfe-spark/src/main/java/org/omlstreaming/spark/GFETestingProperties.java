package org.omlstreaming.spark;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

public class GFETestingProperties {

  ArrayList<String> result;
  InputStream inputStream;

  public GFETestingProperties() {
    result = new ArrayList<>();
    inputStream = null;
  }

  public ArrayList<String> getPropValues() throws IOException {

    try {
      Properties prop = new Properties();

      prop.load(GFETestingProperties.class.getResourceAsStream("/perf_test_config.properties"));

      // get the property value and print it out
      String gfeFeatures = prop.getProperty("features");
      String gfeBackend = prop.getProperty("backend");
      String gfeInfra = prop.getProperty("infrastructure");
      String gfePort = prop.getProperty("cport");
      String gfeDebug = prop.getProperty("debug");

      result.add(gfeFeatures);
      result.add(gfeBackend);
      result.add(gfeInfra);
      result.add(gfePort);
      result.add(gfeDebug);

    } catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }
}
