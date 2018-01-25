package com.projects.thread;

import java.util.concurrent.*;

/**
 * Created by lancerlin on 2017/12/26.
 */
public class FutureTest {

    public static void main(String[] args) {
        FutureTest futureTest = new FutureTest();
        futureTest.useExecutor();
        futureTest.useThread();
    }

    private void useExecutor() {

        SumTask sumTask = new SumTask(1000);
        ExecutorService executor = Executors.newCachedThreadPool();

        FutureTask<Integer> futureTask = new FutureTask<Integer>(sumTask);

        executor.submit(futureTask);
        executor.shutdown();

        try {
            System.out.println(Thread.currentThread().getName()+"::useExecutor运行结果" + futureTask.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void useThread() {
        SumTask sumTask = new SumTask(500);

        FutureTask<Integer> futureTask1 = new FutureTask<Integer>(sumTask);
        FutureTask<Integer> futureTask = new FutureTask<Integer>(sumTask) {
            @Override
            protected void done() {
                super.done();
                try {
                    // 这是在后台线程
                    System.out.println(Thread.currentThread().getName()+"::useThread运行结果" + get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        };

        Thread thread = new Thread(futureTask);
        thread.start();

        try {
            //这是在主线程，会阻塞
            System.out.println(Thread.currentThread().getName()+"::useThread运行结果" + futureTask.get().toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    class SumTask implements Callable<Integer> {
        int number;

        public SumTask(int num) {
            this.number = num;
        }

        @Override
        public Integer call() throws Exception {

            System.out.println(Thread.currentThread().getName());
            Thread.sleep(5000);

            int sum = 0;
            for (int i = 0; i < number; i++) {
                sum += i;
            }
            return sum;
        }
    }
}
