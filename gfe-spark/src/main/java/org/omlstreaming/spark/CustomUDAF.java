package org.omlstreaming.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CustomUDAF extends UserDefinedAggregateFunction {

  private StructType inputSchema, bufferSchema;

  public CustomUDAF() {
    StructField[] inputFields = {
        new StructField("x", DataTypes.DoubleType, false, Metadata.empty()),
        new StructField("y", DataTypes.DoubleType, false, Metadata.empty())
    };
    inputSchema = new StructType(inputFields);

    StructField[] bufferFields = {
        new StructField("covar", DataTypes.DoubleType, false, Metadata.empty()),
        new StructField("n", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("avg_x", DataTypes.DoubleType, false, Metadata.empty()),
        new StructField("avg_y", DataTypes.DoubleType, false, Metadata.empty()),
        new StructField("m2", DataTypes.DoubleType, false, Metadata.empty()),
    };
    bufferSchema = new StructType(bufferFields);
  }

  @Override
  public StructType inputSchema() {
    return inputSchema;
  }

  @Override
  public StructType bufferSchema() {
    return bufferSchema;
  }

  @Override
  public DataType dataType() {
    return DataTypes.DoubleType;
  }

  @Override
  public boolean deterministic() {
    return true;
  }

  @Override
  public void initialize(MutableAggregationBuffer buffer) {
    buffer.update(0, 0.0);
    buffer.update(1, 0);
    buffer.update(2, 0.0);
    buffer.update(3, 0.0);
    buffer.update(4, 0.0);
  }

  /*    n += 1;
    double x = Double.parseDouble(row.getField(1).toString());
    double delta = x - avg_x;
    double y = Double.parseDouble(row.getField(2).toString());
    double delta_y = y - avg_y;
    double delta_n = delta / n;
    double delta_n_y = delta_y / n;
    avg_x += delta_n;
    avg_y += delta_n_y;
    m2 += delta * (delta - delta_n);
    double delta_2 = delta * delta;
    double delta_n_2 = delta_n * delta_n;
    m3 += -3.0 * delta_n * m2 + delta * (delta_2 - delta_n_2);
    m4 += -4.0 * delta_n * m3 - 6.0 * delta_n_2 * m2 + delta * (delta * delta_2
        - delta_n * delta_n_2);
    covar = (n - 2) / (n - 1) * covar + 1 / n * (x - avg_x) * (y - avg_y);
    collector.collect(Row.of(row.getField(0), covar / m2, System.currentTimeMillis()));
    */

  @Override
  public void update(MutableAggregationBuffer buffer, Row input) {
    double x = input.getDouble(1);
    double y = input.getDouble(2);
    double n = buffer.getInt(1) + 1;
    double avg_x =  buffer.getDouble(2);
    double avg_y = buffer.getDouble(3);
    double delta_x = x - avg_x;
    double delta_y = y - avg_y;
    double delta_n_x = delta_x / n;
    double delta_n_y = delta_y / n;
    avg_x += delta_n_x;
    avg_y += delta_n_y;
    double m2 = delta_x * (delta_x - delta_n_x) + buffer.getDouble(4);
    double covar = (n - 2) / (n - 1) * buffer.getDouble(0) + 1 / n * (x - avg_x) * (y - avg_y);
    buffer.update(0, covar);
    buffer.update(1, n);
    buffer.update(2, avg_x);
    buffer.update(3, avg_y);
    buffer.update(4, m2);
  }

  @Override
  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    buffer1.update(0, buffer1.getDouble(0));
    buffer1.update(1, buffer1.getInt(1));
    buffer1.update(2, buffer1.getDouble(2));
    buffer1.update(3, buffer1.getDouble(3));
    buffer1.update(4, buffer1.getDouble(4));
    buffer1.update(5, buffer1.getDouble(5));
  }

  @Override
  public Double evaluate(Row buffer) {
    return buffer.getDouble(0);
  }
}
