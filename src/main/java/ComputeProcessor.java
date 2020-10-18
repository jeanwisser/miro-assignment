import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.SparkHelper;
import utils.PathsHelper;

import java.util.Optional;

import static org.apache.spark.sql.functions.*;

public class ComputeProcessor {
    public static final SparkSession currentSession = new SparkHelper().StartSession("Compute");

    public static void process() {
        final Optional<String> app_loaded_path = PathsHelper.tryGetPath("/app_loaded");
        final Optional<String> registered_path = PathsHelper.tryGetPath("/registered");
        app_loaded_path.ifPresent(p1 -> registered_path.ifPresent(p2 -> {
            Dataset<Row> appLoadedRecords = readParquet(p1);
            Dataset<Row> registeredRecords = readParquet(p2);
            printResult(computePercentageOfActiveUsers(appLoadedRecords, registeredRecords));
        }));
    }

    /**
     * Compute the percentage of users who have loaded the app during the calendar week after registering
     * We first group by user and take the latest registration time in case of duplicates
     * (there is one duplicate in the example)
     * After that we join with the app_loaded events for each user and check if there is an event between (registered_time + 1 day) and the next Sunday
     */
    public static double computePercentageOfActiveUsers(Dataset<Row> appLoadedRecords, Dataset<Row> registeredRecords) {
        Dataset<Row> latestRegistrationByUser = registeredRecords
                .select(col("initiator_id"), col("time"))
                .groupBy(col("initiator_id")).agg(max("time").as("registered_time"));

        long totalNumberOfUsers = latestRegistrationByUser.count();

        long numberOfUsersWhoLoadedAfterRegistration = latestRegistrationByUser.join(appLoadedRecords
                .select(col("initiator_id"), col("time")), "initiator_id")
                .where(col("time").$greater$eq(date_add(col("registered_time"), 1))
                        .and(col("time").$less(next_day(col("registered_time"), "Sun"))))
                .select(col("initiator_id"))
                .distinct()
                .count();

        return ((double) numberOfUsersWhoLoadedAfterRegistration / totalNumberOfUsers) * 100.0;
    }

    private static Dataset<Row> readParquet(String path) {
        return currentSession.read().parquet(path);
    }

    private static void printResult(double activeUsersPercentage) {
        System.out.printf("Percentage of users who loaded the app after registration: %.2f%%%n", activeUsersPercentage);
    }

}
