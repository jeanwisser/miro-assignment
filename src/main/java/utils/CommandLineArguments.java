package utils;

import org.apache.commons.cli.*;

public class CommandLineArguments {

    public enum ExecutionType {
        extract,
        compute
    }

    public static Options getArguments() {
        final Option executionModeOption = Option.builder("m")
                .longOpt("mode")
                .desc("execution_mode: extract | compute")
                .hasArg(true)
                .argName("execution_mode")
                .required(true)
                .build();

        final Options options = new Options();
        options.addOption(executionModeOption);
        return options;
    }
}
