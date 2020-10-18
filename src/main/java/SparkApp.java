import org.apache.commons.cli.*;
import utils.CommandLineArguments;

public class SparkApp {

    public static void main(String[] args) {
        final Options options = CommandLineArguments.getArguments();
        final CommandLineParser parser = new DefaultParser();
        try {
            final CommandLine line = parser.parse(options, args);
            final CommandLineArguments.ExecutionType executionType = CommandLineArguments.ExecutionType.valueOf(line.getOptionValue("m"));
            start(executionType);
        } catch (ParseException e) {
            printHelpAndExit(options, "Missing command line argument");
        } catch (IllegalArgumentException e) {
            printHelpAndExit(options, "The provided execution type is not correct");
        }
    }

    public static void start(CommandLineArguments.ExecutionType executionType) {
        switch (executionType) {
            case extract:
                ExtractProcessor.process();
                break;
            case compute:
                ComputeProcessor.process();
                break;
        }
    }

    private static void printHelpAndExit(Options options, String message) {
        HelpFormatter formatter = new HelpFormatter();
        System.out.println(message);
        formatter.printHelp("SparkApp", options);
        System.exit(1);
    }
}
