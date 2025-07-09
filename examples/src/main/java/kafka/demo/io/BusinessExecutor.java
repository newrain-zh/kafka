package kafka.demo.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BusinessExecutor {

    private static final ExecutorService executor =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    public static void execute(Runnable task) {
        executor.execute(task);
    }
}