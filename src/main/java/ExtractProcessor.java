import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;

import org.apache.spark.sql.types.StructType;
import spark.SparkHelper;
import utils.PathsHelper;

import java.util.Optional;

public class ExtractProcessor {
    public static final SparkSession currentSession = new SparkHelper().StartSession("Extract");

    public static void process() {
        final Optional<String> path = PathsHelper.tryGetPath("/src/main/resources/dataset.json");
        path.ifPresent(p -> {
            Dataset<Row> source = readEventRecords(p).persist();
            writeParquet(getAppLoadedEvents(source), "app_loaded");
            writeParquet(getRegisteredEvents(source), "registered");
        });
    }

    /**
     * Filter the source dataset if event is of type "app_loaded" and select only needed columns
     * Rename timestamp to time to fit the specifications
     */
    public static Dataset<Row> getAppLoadedEvents(Dataset<Row> events) {
        return events
                .filter(col("event").equalTo("app_loaded"))
                .select(col("timestamp"), col("initiator_id"), col("device_type"))
                .withColumnRenamed("timestamp", "time");
    }

    /**
     * Filter the source dataset if event is of type "registered" and select only needed columns
     * Rename timestamp to time to fit the specifications
     */
    public static Dataset<Row> getRegisteredEvents(Dataset<Row> events) {
        return events
                .filter(col("event").equalTo("registered"))
                .select(col("timestamp"), col("initiator_id"), col("channel"))
                .withColumnRenamed("timestamp", "time");
    }

    private static Dataset<Row> readEventRecords(String path) {
        StructType schema = new StructType()
                .add("browser_version", StringType, true)
                .add("campaign", StringType, true)
                .add("channel", StringType, true)
                .add("device_type", StringType, true)
                .add("event", StringType, true)
                .add("initiator_id", LongType, true)
                .add("timestamp", TimestampType, true);

        return currentSession.read()
                .option("header", true)
                .option("mode", "DROPMALFORMED")
                .schema(schema)
                .json(path);
    }

    private static void writeParquet(Dataset<Row> data, String folderName) {
        data.write().format("parquet").mode("overwrite").save(folderName);
    }
}
