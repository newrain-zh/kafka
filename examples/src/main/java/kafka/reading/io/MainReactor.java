package kafka.reading.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class MainReactor implements Runnable {
    private final Selector selector;
    private final ServerSocketChannel serverSocketChannel;

    public MainReactor(int port) throws IOException {
        // 创建主 Reactor 的 Selector
        selector = Selector.open();
        // 配置 ServerSocketChannel
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        // 注册 Accept 事件到主 Reactor
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                selector.select(); // 阻塞等待事件
                Set<SelectionKey>      selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> it           = selectedKeys.iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (key.isAcceptable()) {
                        // 处理新连接（交给子 Reactor）
                        handleAccept(key);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel       clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        // 将新连接分配给子 Reactor（SubReactor）
        SubReactor.subReactorPool.dispatch(clientChannel);
    }
}