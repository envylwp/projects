package com.projects.threadlocal;

/**
 * Created by lancerlin on 2018/2/23.
 */
public class ThreadLocalTest {

    public static ThreadLocal<Integer> threadLocal = new ThreadLocal<Integer>();

    public static void main(String args[]){
        threadLocal.set(new Integer(123));

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
