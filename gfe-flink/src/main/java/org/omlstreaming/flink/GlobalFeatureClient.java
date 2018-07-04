package org.omlstreaming.flink;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

class GFETestingProperties {

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

public class GlobalFeatureClient {

  public static void main(String[] args) throws Exception {

    // get the testing configuration
    GFETestingProperties testConf = new GFETestingProperties();
    ArrayList<String> paramTest = testConf.getPropValues();

    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    GlobalFeatureProfilerDebug debugFlag = new GlobalFeatureProfilerDebug();

    // GFE test parametrization
    String[] featuresList = paramTest.get(0).split(",");
    String backendType = paramTest.get(1);
    String infraType = paramTest.get(2);
    debugFlag.setEnableProfiling(paramTest.get(4).matches("true"));
    int commPort = Integer.valueOf(paramTest.get(3));

    // time window to measure throughput
    long winSize = 1000; //ms

    if (backendType.equals("Local")) {
      // disk file for local data accesses
      final String filePath = "./dataDisk";
      int iFile = 1;
      while (Files.exists(Paths.get(filePath + iFile))) {
        Files.deleteIfExists(Paths.get(filePath + iFile));
        iFile++;
      }
    }
    // Create typed global features
    String[] gfTypeNames = {"Long", "Double", "Double", "Double"};
    String[] gfNamesTyped = {"ts", "featx", "featy", "featnonstat"};

    // performance evaluation dataset
    DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
    Date date = new Date();

    String resEvalFile = "gfe_output_flink_" + args[0] + "_" + dateFormat.format(date) + "/" +
        infraType + "/" +
        backendType + "/" +
        dateFormat.format(date) + "_latency_" +
        Stream.of(featuresList).collect(Collectors.joining()) + "_" +
        gfNamesTyped[1] + "_" +
        gfTypeNames[1];
    String thrEvalFile = "gfe_output_flink_" + args[0] + "_" + dateFormat.format(date) + "/" +
        infraType + "/" +
        backendType + "/" +
        dateFormat.format(date) + "_throughput_" +
        Stream.of(featuresList).collect(Collectors.joining()) + "_" +
        gfNamesTyped[1] + "_" +
        gfTypeNames[1];


    DataStream<Row> outputF;
    int field;
    if (args.length == 1 && args[0] != null && args[0].equals("400000")) {
//    if (Arrays.asList(featuresList).contains("LLS")) {
      // get the content of the input stream, append event receipt time and run feature extractor
      outputF = env.socketTextStream("127.0.0.1", commPort)
          .map(new MapFunction<String, Row>() {
            @Override
            public Row map(String arg0) throws Exception {
              String[] parsed = arg0.split(",");
              // parse the stream
              return Row.of(
                  Long.parseLong(parsed[0]), // stream ts
                  Double.parseDouble(parsed[1]),// value x
                  Double.parseDouble(parsed[2]), // value y
                  Double.parseDouble(parsed[3]), // values for change detector (score,val)
                  System.currentTimeMillis()
              );
            }
          })
          .keyBy(new KeySelector<Row, Object>() {
            @Override
            public Object getKey(Row row) throws Exception {
              return 0L;
            }
          }).process(new LLSProcessFunction()).setParallelism(1);
      field = 2;
    } else {

      // get the content of the input stream, append event receipt time and run feature extractor
      outputF = env.socketTextStream("127.0.0.1", commPort)
          .map(new MapFunction<String, Row>() {
            @Override
            public Row map(String arg0) throws Exception {
              String[] parsed = arg0.split(",");
              // parse the stream
              return Row.of(
                  Long.parseLong(parsed[0]), // stream ts
                  Double.parseDouble(parsed[1]),// value x
                  Double.parseDouble(parsed[2]), // value y
                  Double.parseDouble(parsed[3]), // values for change detector (score,val)
                  System.currentTimeMillis()
              );
                          }
          })
          .keyBy(new KeySelector<Row, Object>() {
            @Override
            public Object getKey(Row row) throws Exception {
              return 0L;
            }
          }).process(new MyProcessFunction()).setParallelism(1);
      field = 4;

    }

    SingleOutputStreamOperator<Row> latency = outputF.map(
        new MapFunction<Row, Row>() {
          @Override
          public Row map(Row row) throws Exception {
            return Row.of(Long.parseLong(row.getField(field).toString()) - Long
                .parseLong(row.getField(0).toString()));
          }
        });
    latency.writeAsText("file:///home/flink/tests_perf_daniele/" + resEvalFile,
        FileSystem.WriteMode.NO_OVERWRITE);

    // sink to measure throughput
    SingleOutputStreamOperator<Row> throughput = outputF
        .timeWindowAll(Time.milliseconds(winSize))
        .apply(new AllWindowFunction<Row, Row, TimeWindow>() {
          @Override
          public void apply(TimeWindow timeWindow,
              Iterable<Row> iterable,
              Collector<Row> collector) throws Exception {
            // calculate throughput computation
            Iterator<Row> iterFld = iterable.iterator();
            long count = 0;
            while (iterFld.hasNext()) {
              iterFld.next();
              count++;
            }
            Row result = new Row(1);
            result.setField(0, count);
            collector.collect(result);
          }
        })
        .setParallelism(1);
    throughput
        .writeAsText("file:///home/flink/tests_perf_daniele/" + thrEvalFile,
            FileSystem.WriteMode.NO_OVERWRITE);

    // execute program
    JobExecutionResult jobRes = env.execute("General Feature Extractor Tester");

//    if (jobRes.isJobExecutionResult()) {
//
//      // add headers for the output logs
//      FileWriter featValWriter = new FileWriter(resEvalFile + ".csv");
//      FileReader featValReader = new FileReader(resEvalFile);
//      String featValWriterHeader = "LatencyProbed(ms),LatencySumProbedStages(ms)\n";
//      featValWriter.write(featValWriterHeader);
//      int rdAll = featValReader.read();
//      while (rdAll != -1) {
//        featValWriter.write(rdAll);
//        rdAll = featValReader.read();
//      }
//      featValWriter.flush();
//      featValWriter.close();
//
//      FileWriter thrTimeWriter = new FileWriter(thrEvalFile + ".csv");
//      FileReader thrTimeReader = new FileReader(thrEvalFile);
//      String thrTimeWriterHeader = "Throughput(ev/s)\n";
//      thrTimeWriter.write(thrTimeWriterHeader);
//      rdAll = thrTimeReader.read();
//      while (rdAll != -1) {
//        thrTimeWriter.write(rdAll);
//        rdAll = thrTimeReader.read();
//      }
//      thrTimeWriter.flush();
//      thrTimeWriter.close();
//
//      // cleanup
//      Files.deleteIfExists(Paths.get(resEvalFile));
//      Files.deleteIfExists(Paths.get(thrEvalFile));
//    }
  }
}