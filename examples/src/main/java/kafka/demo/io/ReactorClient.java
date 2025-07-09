package kafka.demo.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ReactorClient {
    public static void main(String[] args) throws IOException {
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("localhost", 8080));
        ByteBuffer buffer = ByteBuffer.wrap("Hello Reactor!".getBytes());
        channel.write(buffer);
        channel.close();
    }
}