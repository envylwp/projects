package com.projects.threadlocal;

/**
 * Created by lancerlin on 2018/2/23.
 */
public class InheritableThreadLocalTest {
    public static InheritableThreadLocal<Integer> threadLocal = new InheritableThreadLocal<Integer>();

    public static void main(String args[]){
        threadLocal.set(new Integer(1234));

        Thread thread = new MyThread();
        thread.start();

        System.out.println("main = " + threadLocal.get());


        Thread thread1 = Thread.currentThread();
        System.out.println(thread1);
        System.out.println(thread);
    }

    static class MyThread extends Thread{
        @Override
        public void run(){
            System.out.println("MyThread = " + threadLocal.get());
        }
    }
}
