package com.projects.mianshi;

/**
 * Created by lancerlin on 2018/1/16.
 * <p>
 * 甲乙两地相距100公里，有一辆火车以每小时15公里的速度离开甲地直奔乙地，另一辆火车以每小时20公里的速度从乙地开往甲地。
 * 如果有一只鸟，以30公里每小时的速度和两辆火车同时启动，从甲地出发，碰到另一辆车后返回，依次在两辆火车来回飞行，直到两辆火车相遇，
 * 请问，这只小鸟往返了多少次？
 */


public class TrainBirdsCount {

    public static void main(String[] args) {
        double s = 100.0;
        int v0 = 15;
        int v1 = 20;
        int vb = 30;
        boolean flag = true;
        int n = 0;

        while (s > 0.0000000000001) {

            if (flag) {
                s = s - s / (vb + v1) * (v0 + v1);
                n++;
                flag = false;
            } else {
                s = s - s / (vb + v0) * (v0 + v1);
                n ++;
                flag = true;

            }
        }
        System.out.println(n);
    }

}
