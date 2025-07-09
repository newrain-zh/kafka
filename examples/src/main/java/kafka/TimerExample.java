package kafka;

import org.apache.kafka.common.utils.Time;

public class TimerExample {
    public static void main(String[] args) {
        long startMs = Time.SYSTEM.hiResClockMs();
        long tickMs = 20L;
        long currentTimeMs = startMs - (startMs % tickMs);
        System.out.println(currentTimeMs);
    }
}