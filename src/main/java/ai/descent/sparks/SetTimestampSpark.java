package ai.descent.sparks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.spark.sql.functions.callUDF;

public class SetTimestampSpark extends SparkDescentBase {

    public static void main(String[] args) {
        new SetTimestampSpark().run();
    }

    public SetTimestampSpark() {
        super(SetTimestampSpark.class.getName());
    }

    @Override
    public void run() {

        UDF2<Date, String, Timestamp> createTimestamp =
            (Date date, String HHmm) -> {
                Calendar c = new GregorianCalendar(TimeZone.getTimeZone("America/New_York"));
                c.setTime(date);
                c.set(Calendar.HOUR_OF_DAY, Integer.parseInt(HHmm.substring(0,2)));
                c.set(Calendar.MINUTE, Integer.parseInt(HHmm.substring(2,4)));
                return new Timestamp(c.getTime().getTime());
            };

        getSpark().udf().register("createTimestamp", createTimestamp, DataTypes.TimestampType);

        Dataset<Row> ds = getDataFrameReader("(select tick_date, tick_time from usdcad) x");

        save(ds.withColumn("ts",
                callUDF("createTimestamp", ds.col("tick_date"), ds.col("tick_time"))));
    }

    private void save(Dataset<Row> rowDataset) {
        final Connection conn = getConnection();
        final int max = 500;
        final AtomicInteger batchSize = new AtomicInteger(0);
        final AtomicInteger numBatches = new AtomicInteger(0);
        final Row[] rows = (Row[])rowDataset.collect();

        try (PreparedStatement ps = conn.prepareStatement("update usdcad set ts = ? where tick_date = ? and tick_time = ?")) {
            for (Row row : rows) {
                if (batchSize.get() < max) {
                    batchSize.addAndGet(1);
                    Timestamp ts = row.getAs("ts");

                    ps.setTimestamp(1, ts);
                    ps.setDate(2, row.getAs("tick_date"));
                    ps.setString(3, row.getAs("tick_time"));
                    ps.addBatch();
                } else {
                    System.out.println("Batch " + numBatches.getAndIncrement());
                    ps.executeBatch();
                    ps.clearParameters();
                    batchSize.set(0);
                }
            };
            if (batchSize.get() > 0) {
                ps.executeBatch();
            }
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
