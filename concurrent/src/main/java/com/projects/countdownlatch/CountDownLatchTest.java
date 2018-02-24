package com.projects.countdownlatch;

import java.util.concurrent.CountDownLatch;

/**
 * Created by lancerlin on 2018/2/24.
 */
public class CountDownLatchTest {

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch doneSignal = new CountDownLatch(5);

        // 依次创建并启动5个worker线程
        for (int i = 0; i < 5; ++i) {
            new Thread(new Worker(startSignal, doneSignal)).start();
        }

        System.out.println("Driver is doing something...");
        System.out.println("Driver is Finished, start all workers ...");
        startSignal.countDown(); // Driver执行完毕，发出开始信号，使所有的worker线程开始执行
        doneSignal.await(); // 等待所有的worker线程执行结束
        System.out.println("Finished.");
    }

//    作者：JohnShen
//    链接：https://www.jianshu.com/p/1ec1009ebab7
//    來源：简书
//    著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
}

class Worker implements Runnable{
    private final CountDownLatch startSignal;
    private final CountDownLatch doneSignal;
    Worker(CountDownLatch startSignal, CountDownLatch doneSignal) {
        this.startSignal = startSignal;
        this.doneSignal = doneSignal;
    }
    public void run() {
        try {
            startSignal.await(); // 等待Driver线程执行完毕，获得开始信号
            System.out.println("Working now ...");
            doneSignal.countDown(); // 当前worker执行完毕，释放一个完成信号
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
