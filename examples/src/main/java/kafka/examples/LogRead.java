package kafka.examples;

import org.apache.kafka.common.record.*;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class LogRead {
    public static void main(String[] args) throws IOException {

        File        file    = new File("/tmp/kraft-combined-logs/my-topic-0/00000000000000000000.log");
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        FileRecords records = FileRecords.open(file);
        Iterator<FileLogInputStream.FileChannelRecordBatch> iterator = records.batches().iterator();
        while (iterator.hasNext()){
            FileLogInputStream.FileChannelRecordBatch next = iterator.next();
        }
    }
}