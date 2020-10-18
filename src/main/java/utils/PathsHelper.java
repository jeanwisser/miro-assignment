package utils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

public class PathsHelper {
    public static Optional<String> tryGetPath(String path) {
        String currentPath = System.getProperty("user.dir") + path;
        if (Files.exists(Paths.get(currentPath))) {
            return Optional.of(currentPath);
        } else {
            System.out.printf("The path was not found: %s%n", path);
            return Optional.empty();
        }
    }
}
