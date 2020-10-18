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

import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.Assert.assertEquals;

public class ComputeProcessorTests {
    public static final SparkSession currentSession = new SparkHelper().StartSession("Compute");
    private Dataset<Row> appLoaded;
    private Dataset<Row> registered;

    @Before
    public void createRegisteredDataset() {
        List<Row> registeredRows = Arrays.asList(
                RowFactory.create(java.sql.Timestamp.valueOf("2020-10-15 10:10:10.0"), 1L, "invite"),
                RowFactory.create(java.sql.Timestamp.valueOf("2020-10-16 10:10:10.0"), 2L, "invite"),
                RowFactory.create(java.sql.Timestamp.valueOf("2020-10-18 10:10:10.0"), 3L, "invite"),
                RowFactory.create(java.sql.Timestamp.valueOf("2020-10-19 10:10:10.0"), 4L, "invite"),
                RowFactory.create(java.sql.Timestamp.valueOf("2020-10-18 10:10:10.0"), 5L, "invite"));

        StructType registeredSchema = new StructType()
                .add("time", TimestampType, true)
                .add("initiator_id", LongType, true)
                .add("channel", StringType, true);

        registered = currentSession.createDataFrame(registeredRows, registeredSchema);
    }

    @Before
    public void createAppLoadedDataset() {
        List<Row> appLoadedRows = Arrays.asList(
                RowFactory.create(java.sql.Timestamp.valueOf("2020-10-16 10:10:10.0"), 1L, "desktop"),
                RowFactory.create(java.sql.Timestamp.valueOf("2020-10-16 10:10:10.0"), 2L, "desktop"),
                RowFactory.create(java.sql.Timestamp.valueOf("2020-10-19 10:10:10.0"), 3L, "desktop"),
                RowFactory.create(java.sql.Timestamp.valueOf("2020-10-20 10:10:10.0"), 4L, "desktop"));

        StructType appLoadedSchema = new StructType()
                .add("time", TimestampType, true)
                .add("initiator_id", LongType, true)
                .add("device_type", StringType, true);

        appLoaded = currentSession.createDataFrame(appLoadedRows, appLoadedSchema);
    }

    @Test
    public void shouldComputePercentageOfActiveUsers() {
        double result = ComputeProcessor.computePercentageOfActiveUsers(appLoaded, registered);
        assertEquals(3.0 / 5.0 * 100.0, result, 0.000001);
    }
}
