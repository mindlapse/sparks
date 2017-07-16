package ai.descent.sparks;

public class MovingAverageSpark extends SparkDescentBase {

    public static void main(String[] args) {
        new MovingAverageSpark().run();
    }

    public MovingAverageSpark() {
        super(MovingAverageSpark.class.getName());
    }

    @Override
    public void run() {
//        getDataFrameReader().
//                .load().limit(100).select("tick_date", "tick_time", "ts");
//                .withColumn("ts", udf())
//                .map(
//                        row -> {
//                            spark.createDataFrame()
//                            row.schema()
//                            row.
//
//                        }
//                )
//                show();

    }
}
