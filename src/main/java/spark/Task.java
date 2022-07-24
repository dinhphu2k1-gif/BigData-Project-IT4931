package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Task {
    private SparkSession spark;

    private final String sourcePath = "hdfs:/bigdata-project/data";

    private final String destinationPath = "hdfs:/bigdata-project/result";

    public Task(String appName) {
        this.spark = SparkSession.builder()
                .appName(appName)
                .master("yarn")
                .getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");
    }

    /**
     * - Top 10 bài hát được xem nhiều nhất (lấy các trường page = NextSong)
     *
     */
    public void ex1() {
        for (int i = 1; i <= 30; i++) {
            String day;
            if (i < 10) day = "0" + i;
            else day = String.valueOf(i);

            Dataset<Row> df = spark.read().parquet(sourcePath + "/year=2015/month=06/day=" + day);

            df = df.filter("page == 'NextSong'");

            Dataset<Row> res = df.groupBy("song")
                    .agg(count("*").as("count"))
                    .orderBy(desc("count"));

            System.out.println("Top 10 bài hát được xem nhiều nhất trong ngày" + day + "/06/2015");
            res.show(false);

            res.coalesce(1)
                    .write()
                    .option("header","true")
                    .mode(SaveMode.Overwrite)
                    .csv(destinationPath + "/ex1/" + day);
        }
    }

    /**
     * Danh sách người dùng truy cập nhiều nhất trong ngày
     */
    public void ex2() {
        for (int i = 1; i <= 30; i++) {
            String day;
            if (i < 10) day = "0" + i;
            else day = String.valueOf(i);

            Dataset<Row> df = spark.read().parquet(sourcePath + "/year=2015/month=06/day=" + day);

            df = df.filter("userId != ''");

            Dataset<Row> res = df.groupBy("userId")
                    .agg(count("*").as("count"))
                    .orderBy(desc("count"));

            System.out.println("Danh sách người dùng truy cập nhiều nhất trong ngày" + day + "/06/2015");
            res.show(false);

            res.coalesce(1)
                    .write()
                    .option("header","true")
                    .mode(SaveMode.Overwrite)
                    .csv(destinationPath + "/ex2/" + day);
        }
    }

    /**
     * Top 10 thành phố có lượt truy cập nhiều nhất
     */
    public void ex3() {
        for (int i = 1; i <= 30; i++) {
            String day;
            if (i < 10) day = "0" + i;
            else day = String.valueOf(i);

            Dataset<Row> df = spark.read().parquet(sourcePath + "/year=2015/month=06/day=" + day);

            df = df.withColumn("city", split(col("location"), ", ").getItem(1));

            Dataset<Row> res = df.groupBy("city")
                    .agg(count("*").as("count"))
                    .orderBy(desc("count"));

            System.out.println("Top 10 thành phố có lượt truy cập nhiều nhất:" + day + "/06/2015");
            res.show(false);

            res.coalesce(1)
                    .write()
                    .option("header","true")
                    .mode(SaveMode.Overwrite)
                    .csv(destinationPath + "/ex3/" + day);
        }
    }

    public void run() {
        ex1();
        ex2();
        ex3();
    }

    public static void main(String[] args) {
        Task task = new Task("Analysis");
        task.run();
    }
}
