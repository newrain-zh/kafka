package kafka.reading.io;

import java.io.IOException;

public class ReactorServer {
    public static void main(String[] args) {
        try {
            // 启动主 Reactor（监听 8080 端口）
            MainReactor mainReactor = new MainReactor(8080);
            new Thread(mainReactor).start();
            System.out.println("Reactor Server Started on port 8080");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}