import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import spark.SparkHelper;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ExtractProcessorTests {
    public static final SparkSession currentSession = new SparkHelper().StartSession("Extract");
    private Dataset<Row> source;

    @Before
    public void createSourceDataset() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("1.0", "1", "channel_1", "desktop", "app_loaded", 1L, java.sql.Timestamp.valueOf("2020-10-18 10:10:10.0")),
                RowFactory.create("1.0", "1", "channel_1", "desktop", "app_loaded", 2L, java.sql.Timestamp.valueOf("2020-10-18 10:10:10.0")),
                RowFactory.create("1.0", "1", "channel_1", "desktop", "app_loaded", 3L, java.sql.Timestamp.valueOf("2020-10-18 10:10:10.0")),
                RowFactory.create("1.0", "1", "channel_1", "desktop", "registered", 4L, java.sql.Timestamp.valueOf("2020-10-18 10:10:10.0")),
                RowFactory.create("1.0", "1", "channel_1", "desktop", "registered", 5L, java.sql.Timestamp.valueOf("2020-10-18 10:10:10.0")));

        StructType schema = new StructType()
                .add("browser_version", StringType, true)
                .add("campaign", StringType, true)
                .add("channel", StringType, true)
                .add("device_type", StringType, true)
                .add("event", StringType, true)
                .add("initiator_id", LongType, true)
                .add("timestamp", TimestampType, true);

        source = currentSession.createDataFrame(rows, schema);
    }

    @Test
    public void shouldFilterAppLoaded() {
        Dataset<Row> result = ExtractProcessor.getAppLoadedEvents(source);
        assertEquals(result.count(), 3);
        assertFalse(result.filter(col("initiator_id").equalTo(1L)).isEmpty());
        assertFalse(result.filter(col("initiator_id").equalTo(2L)).isEmpty());
        assertFalse(result.filter(col("initiator_id").equalTo(3L)).isEmpty());
    }

    @Test
    public void shouldFilterRegistered() {
        Dataset<Row> result = ExtractProcessor.getRegisteredEvents(source);
        assertEquals(result.count(), 2);
        assertFalse(result.filter(col("initiator_id").equalTo(4L)).isEmpty());
        assertFalse(result.filter(col("initiator_id").equalTo(5L)).isEmpty());
    }
}
