package com.projects.guava.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class CacheCallableCallbackDemo1 {
    public static void main(String[] args) throws ExecutionException {
        Cache<String, String> cache = CacheBuilder.newBuilder().maximumSize(1000).build();
        String resultVal = cache.get("a", new Callable<String>() {
            public String call() {
                String strProValue = "hello " + "a" + "!";
                return strProValue;
            }
        });
        System.out.println("a value : " + resultVal);

        resultVal = cache.getIfPresent("b");
        System.out.println("a value : " + resultVal);
    }
}
