package org.omlstreaming.flink;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class MyProcessFunction extends ProcessFunction<Row, Row> {

  private double n = 0;
  private double mean = 0;
  private double m2 = 0.0;
  private double m3 = 0.0;

  @Override
  public void processElement(Row row, Context context, Collector<Row> collector) throws Exception {
    n += 1;
    double delta = Double.parseDouble(row.getField(1).toString()) - mean;
    double delta_n = delta / n;
    mean += delta_n;
    m2 += delta * (delta - delta_n);
    double delta_2 = delta * delta;
    double delta_n_2 = delta_n * delta_n;
    m3 += -3.0 * delta_n * m2 + delta * (delta_2 - delta_n_2);
    collector.collect(
        Row.of(
            row.getField(4),
            mean,
            m2 / n,
            (Math.sqrt(n) * m3) / Math.sqrt(Math.pow(m3, 3)),
            System.currentTimeMillis()
        )
    );
  }
}
