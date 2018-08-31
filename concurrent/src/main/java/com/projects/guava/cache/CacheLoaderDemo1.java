package com.projects.guava.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class CacheLoaderDemo1 {
    public static void main(String[] args) throws ExecutionException {
        LoadingCache<String, String> cahceBuilder = CacheBuilder.newBuilder().build(new CacheLoader<String, String>() {
            @Override
            public String load(String key) throws Exception {
                System.out.println("请求的key为" + key + "在缓存中不存在,通过load方法获取key.");
                String strProValue = "hello " + key + "!";
                return strProValue;
            }
        });
        // 第一次到缓存里面key为peida的数据，缓存不存在通过，load加载，并保存到缓存里面
        System.out.println("a value:" + cahceBuilder.get("a"));
        // 第二次获取key为peida的数据，缓存已经存在，直接在缓存里面返回
        System.out.println("a value:" + cahceBuilder.get("a"));

        // 往缓存里面存放数据
        cahceBuilder.put("b", "bbbb");
        // 缓存已经存在数据了,直接获取
        System.out.println("b value:" + cahceBuilder.get("b"));
        cahceBuilder.put("b", "bbbbv2");
        System.out.println("b value:" + cahceBuilder.get("b"));

        ConcurrentMap<String, String> map = cahceBuilder.asMap();
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            System.out.println(entry.getKey() + " ========== " + entry.getValue());
        }
    }
}
