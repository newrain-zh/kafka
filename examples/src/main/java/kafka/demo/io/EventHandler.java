package kafka.demo.io;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface EventHandler {


    void handle(SelectionKey key) throws IOException;
}