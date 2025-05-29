package kafka.reading.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SubReactor implements Runnable {
    private final Selector                        selector;
    private final ConcurrentLinkedQueue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();

    // 子 Reactor 池（静态工厂）
    public static final SubReactorPool subReactorPool = new SubReactorPool(4);

    public SubReactor() throws IOException {
        selector = Selector.open();
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                // 处理异步任务（如注册新 Channel）
                processTaskQueue();
                // 处理 I/O 事件
                selector.select(500); // 非阻塞等待最多 500ms
                Set<SelectionKey>      selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> it           = selectedKeys.iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processTaskQueue() {
        Runnable task;
        while ((task = taskQueue.poll()) != null) {
            task.run(); // 执行注册 Channel 等任务
        }
    }

    // 注册新 Channel 到当前 SubReactor
    public void register(SocketChannel channel) {
        taskQueue.add(() -> {
            try {
                channel.register(selector, SelectionKey.OP_READ);
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            }
        });
        selector.wakeup(); // 唤醒阻塞的 select()
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel   = (SocketChannel) key.channel();
        ByteBuffer    buffer    = ByteBuffer.allocate(1024);
        int           readBytes = channel.read(buffer);
        if (readBytes > 0) {
            // 提交到业务线程池处理
            BusinessExecutor.execute(() -> processRequest(buffer));
        } else if (readBytes < 0) {
            channel.close();
        }
    }

    private void processRequest(ByteBuffer buffer) {
        // 业务处理逻辑（例如解析请求、生成响应）
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String request = new String(bytes);
        System.out.println("Process request: " + request);
    }

    private void handleWrite(SelectionKey key) throws IOException {
        // 处理写事件（示例省略具体逻辑）
    }
}

// 子 Reactor 池（负载均衡）
class SubReactorPool {
    private final SubReactor[] reactors;
    private       int          nextIndex = 0;

    public SubReactorPool(int poolSize) {
        reactors = new SubReactor[poolSize];
        for (int i = 0; i < poolSize; i++) {
            try {
                reactors[i] = new SubReactor();
                new Thread(reactors[i]).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 轮询分配 SubReactor
    public void dispatch(SocketChannel channel) {
        reactors[nextIndex].register(channel);
        nextIndex = (nextIndex + 1) % reactors.length;
    }
}