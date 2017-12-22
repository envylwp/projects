package com.projects.concurrent.volatiles;

/**
 * Created by lancerlin on 2017/12/22.
 */
public class NumberRange {
    private int lower, upper;

    public int getLower() {
        return lower;
    }

    public int getUpper() {
        return upper;
    }

    public void setLower(int value) {
        if (value > upper) {

            throw new IllegalArgumentException("...");
        }
        lower = value;
    }

    public void setUpper(int value) {
        if (value < lower) {
            throw new IllegalArgumentException("...");

        }
        upper = value;
    }

    volatile boolean shutdownRequested;
    public void shutdown() { shutdownRequested = true; }
    public void doWork() {
        while (!shutdownRequested) {
            // do stuff
        }
    }}
