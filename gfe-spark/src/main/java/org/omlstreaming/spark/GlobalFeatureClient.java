package org.omlstreaming.spark;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class GlobalFeatureClient {

  public static void main(String[] args) throws Exception {

    BasicConfigurator.configure(new NullAppender());

    // get the testing configuration
    GFETestingProperties testConf = new GFETestingProperties();
    ArrayList<String> paramTest = testConf.getPropValues();

    // set up the streaming execution environment
    final SparkSession spark = SparkSession.builder().appName("GFE")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/").getOrCreate();

    // GFE test parametrization
    int commPort = Integer.valueOf(paramTest.get(3));

    // performance evaluation dataset
    DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
    Date date = new Date();

    String resEvalFile =
        "gfe_output_spark_" + args[0] + "_" + dateFormat.format(date) + "_latency_";
    String thrEvalFile =
        "gfe_output_spark_" + args[0] + "_" + dateFormat.format(date) + "_throughput_";

    UDF0<Long> addtime = (UDF0<Long>) System::currentTimeMillis;

    UDF0<Timestamp> addtimestamp = (UDF0<Timestamp>) () -> new Timestamp(
        System.currentTimeMillis());

    UDF1<Long, Long> calculate = (UDF1<Long, Long>) elem -> System.currentTimeMillis() - elem;

    UDF2<Long, Long, Long> difference = (UDF2<Long, Long, Long>) (long1, long2) -> Math
        .abs(long1 - long2);

    spark.sqlContext().udf().register("addtime", addtime, DataTypes.LongType);
    spark.sqlContext().udf().register("addtimestamp", addtimestamp, DataTypes.TimestampType);
    spark.sqlContext().udf().register("calculate", calculate, DataTypes.LongType);
    spark.sqlContext().udf().register("difference", difference, DataTypes.LongType);

    // get the content of the input stream, append event receipt time and run feature extractor
    Dataset<Row> outputF = spark.readStream().format("socket")
        .option("host", "127.0.0.1")
        .option("port", commPort)
        .load()
        .as(Encoders.STRING())
        .map((MapFunction<String, StdClass>) arg0 -> {
          String[] parsed = arg0.split(",");
          // parse the stream
          return new StdClass(
              Long.parseLong(parsed[0]), // stream ts
              new Timestamp(System.currentTimeMillis()), // stream ts
              Double.parseDouble(parsed[1]),// value x
              Double.parseDouble(parsed[2]), // value y
              Double.parseDouble(parsed[3]), // values for change detector (score,val)
              System.currentTimeMillis()
          );
        }, Encoders.bean(StdClass.class))
        .withColumn("timeCalc", callUDF("calculate", col("time1")))
        .withWatermark("timestamp", "2 second");

    StreamingQuery queryThroughput, queryLatency;
    Dataset<Row> result;
    if (args.length == 1 && args[0] != null && args[0].equals("400000")) {
      // 400 000 events for LLS
      spark.sqlContext().udf().register("LLS", new CustomUDAF());
      outputF.createOrReplaceTempView("t");
      result = spark
          .sql(
              "SELECT ts, timestamp, time1 LLS(x, y) as lls FROM t GROUP BY ts, timestamp, timeCalc, time1");

    } else {
      // 303 000 000 events for AVG, VAR, SKEW
      result = outputF
          .groupBy(col("ts"), col("timestamp"), col("timeCalc"), col("time1")).agg(
              avg(col("x")),
              variance(col("y")),
              skewness(col("x"))
          );
    }

    result = result.map((MapFunction<Row, StdClass>) row -> {
      // parse the stream
      return new StdClass(
          row.getLong(row.fieldIndex("time1")), new Timestamp(0L), 0.0, 0.0, 0.0,
          System.currentTimeMillis()
      );
    }, Encoders.bean(StdClass.class))
        .withColumn("diff", callUDF("difference", col("time1"), col("ts")));

    queryLatency = result
        .select(col("time1"), col("ts"), col("diff"))
        .writeStream()
        .trigger(Trigger.ProcessingTime(1L))
        .format("csv")
        .option("path", "file:///home/flink/tests_perf_daniele/" + resEvalFile)
        .start();

    queryThroughput = result
        .select(col("time1"))
        .writeStream()
        .format("csv")
        .option("path", "file:///home/flink/tests_perf_daniele/" + thrEvalFile)
        .start();

    try {
      queryLatency.awaitTermination();
      queryThroughput.awaitTermination();
    } catch (StreamingQueryException e) {
      e.printStackTrace();
    }
  }
}
