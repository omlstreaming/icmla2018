package org.omlstreaming.flink;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class LLSProcessFunction extends ProcessFunction<Row, Row> {

  private double n = 0;
  private double avg_x = 0.0;
  private double avg_y = 0.0;
  private double covar = 0.0;
  private double m2 = 0.0;

  @Override
  public void processElement(Row row, Context context, Collector<Row> collector) throws Exception {
    n += 1;
    double x = Double.parseDouble(row.getField(1).toString());
    double delta = x - avg_x;
    double y = Double.parseDouble(row.getField(2).toString());
    double delta_y = y - avg_y;
    double delta_n = delta / n;
    double delta_n_y = delta_y / n;
    avg_x += delta_n;
    avg_y += delta_n_y;
    m2 += delta * (delta - delta_n);
    covar = (n - 2) / (n - 1) * covar + 1 / n * (x - avg_x) * (y - avg_y);
    collector.collect(Row.of(row.getField(4), covar / m2, System.currentTimeMillis()));
  }
}
