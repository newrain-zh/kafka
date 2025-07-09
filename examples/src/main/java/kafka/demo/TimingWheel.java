package kafka.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

// 定时任务接口
interface TimerTask {
    void execute();

    boolean isCancelled();

    void cancel();

    long getExpiration();
}

// 定时任务实现
class TimerTaskImpl implements TimerTask {
    private final long     delayMs;
    private final Runnable callback;
    private       long     expiration;
    private       boolean  cancelled;

    public TimerTaskImpl(long delayMs, Runnable callback) {
        this.delayMs    = delayMs;
        this.callback   = callback;
        this.expiration = 0;
        this.cancelled  = false;
    }

    @Override
    public void execute() {
        if (!cancelled) {
            callback.run();
        }
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void cancel() {
        this.cancelled = true;
    }

    @Override
    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    public long getDelayMs() {
        return delayMs;
    }
}

// 时间轮实现
public class TimingWheel {
    private final long                                   tickMs;          // 时间刻度(毫秒)
    private final int                                    wheelSize;        // 时间轮大小(槽数量)
    private final long                                   interval;        // 时间轮总间隔(毫秒)
    private final AtomicLong                             currentTime; // 当前时间(毫秒)
    private final List<ConcurrentLinkedQueue<TimerTask>> slots; // 时间槽列表
    private final ScheduledExecutorService               scheduler; // 定时器
    private final ReentrantLock                          lock = new ReentrantLock(); // 重入锁
    private       ScheduledFuture<?>                     tickFuture; // 定时任务句柄

    public TimingWheel(long tickMs, int wheelSize) {
        this.tickMs      = tickMs;
        this.wheelSize   = wheelSize;
        this.interval    = tickMs * wheelSize;
        this.currentTime = new AtomicLong(getCurrentTimeMs() / tickMs * tickMs); // 向下取整

        // 初始化时间槽
        slots = new ArrayList<>(wheelSize);
        for (int i = 0; i < wheelSize; i++) {
            slots.add(new ConcurrentLinkedQueue<>());
        }

        // 创建定时器
        scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r);
            t.setName("TimingWheel-TickThread");
            t.setDaemon(true);
            return t;
        });
    }

    // 获取当前时间(毫秒)
    private long getCurrentTimeMs() {
        return System.currentTimeMillis();
    }

    // 启动时间轮
    public void start() {
        if (tickFuture == null) {
            tickFuture = scheduler.scheduleAtFixedRate(this::advanceClock, tickMs, tickMs, TimeUnit.MILLISECONDS);
        }
    }

    // 停止时间轮
    public void stop() {
        if (tickFuture != null) {
            tickFuture.cancel(false);
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }
    }

    // 添加定时任务
    public boolean addTask(TimerTask task) {
        lock.lock();
        try {
            // 设置任务到期时间
            TimerTaskImpl taskImpl = (TimerTaskImpl) task;
            taskImpl.setExpiration(getCurrentTimeMs() + taskImpl.getDelayMs());

            // 如果任务已过期，立即执行
            if (taskImpl.getExpiration() <= currentTime.get() + tickMs) {
                new Thread(task::execute).start();
                return true;
            }

            // 计算任务应该放入的槽
            int virtualId = (int) (taskImpl.getExpiration() / tickMs);
            int index     = virtualId % wheelSize;

            // 添加到对应槽
            slots.get(index).add(task);
            return true;
        } finally {
            lock.unlock();
        }
    }

    // 推进时钟并执行到期任务
    private void advanceClock() {
        lock.lock();
        try {
            // 更新当前时间
            currentTime.addAndGet(tickMs);

            // 获取当前槽索引
            long virtualId = currentTime.get() / tickMs;
            int  index     = (int) (virtualId % wheelSize);

            // 获取当前槽的所有任务
            ConcurrentLinkedQueue<TimerTask> bucket = slots.get(index);
            TimerTask                        task;

            // 执行所有到期任务
            while ((task = bucket.poll()) != null) {
                if (task.getExpiration() <= currentTime.get()) {
                    new Thread(task::execute).start();
                } else {
                    // 如果任务未到期，重新添加
                    addTask(task);
                }
            }
        } catch (Exception e) {
            System.err.println("Error in advanceClock: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    // 使用示例
    public static void main(String[] args) throws InterruptedException {
        // 创建时间轮(刻度100ms，大小20)
        TimingWheel timeWheel = new TimingWheel(100, 20);
        timeWheel.start();

        // 添加任务
        timeWheel.addTask(new TimerTaskImpl(1000, () -> System.out.println("Task 1 executed at " + System.currentTimeMillis())));
        timeWheel.addTask(new TimerTaskImpl(2500, () -> System.out.println("Task 2 executed at " + System.currentTimeMillis())));
        timeWheel.addTask(new TimerTaskImpl(5000, () -> System.out.println("Task 3 executed at " + System.currentTimeMillis())));

        // 运行一段时间后停止
        Thread.sleep(6000);
        timeWheel.stop();
    }
}