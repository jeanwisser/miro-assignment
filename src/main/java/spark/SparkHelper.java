package spark;

import org.apache.spark.sql.SparkSession;

public class SparkHelper {
    public SparkSession StartSession(String appName) {
        return SparkSession.builder()
                .appName(appName)
                .master("local[*]")
                .getOrCreate();
    }
}
