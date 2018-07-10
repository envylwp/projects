package com.projects.regex;

import java.io.File;
import java.io.FileFilter;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by lancerlin on 2018/4/13.
 */
public class TegTest {
    public static void main(String[] args) throws Exception {

//        String path = "/home/(.*)/x";
//
//        Pattern compile = Pattern.compile(path);
//        //去除()，为了截取两个正则的字符串
//        String s = path.replaceAll("\\(|\\)", "").replace("\\", "");
//        System.out.println(s);
//        Matcher matcher = compile.matcher(s);
//        matcher.matches();
//        //g1   fsof_.*    g2 .*.txt
//        String g1 = matcher.group(1);
//        String g2 = matcher.group(2);
//        int indexOf = s.indexOf(g1);
//        //截取g1前一部分
//        String s1 = s.substring(0, indexOf);
//        //截取g1和g2之间的字符串
//        String s2 = s.substring(indexOf + g1.length(), s.lastIndexOf("/") + 1);
//
//
//        System.out.println(s1);
//        System.out.println(s2);
//        System.out.println(g1);
//        System.out.println(g2);
//
//        System.out.println(compile.matcher("/home/(info)/(info_11.log)"));

        String path = "E:/home/product/logs/(.*)/(info_\\[0-9\\]\\{10\\}\\.log)";
        Set<String> allMatchFile = getAllMatchFile(path);
        System.out.println(allMatchFile);

    }


    public static Set<String> getAllMatchFile(String path) throws Exception{
        Pattern compile = Pattern.compile(path);
        //去除()，为了截取两个正则的字符串
        String s = path.replaceAll("\\(|\\)", "").replace("\\", "");
        Matcher matcher = compile.matcher(s);
        matcher.matches();
        //g1   fsof_.*    g2 .*.txt
        String g1 = matcher.group(1);
        String g2 = matcher.group(2);
        int indexOf = s.indexOf(g1);
        //截取g1前一部分
        String s1 = s.substring(0, indexOf);
        //截取g1和g2之间的字符串
        String s2 = s.substring(indexOf + g1.length(), s.lastIndexOf("/") + 1);
        //正则，匹配符合g1的文件夹
        final Pattern g1compile = Pattern.compile(g1);

        File fp1 = new File(s1);
        File[] files = fp1.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                //System.out.println(pathname.getName());
                return pathname.isDirectory() && g1compile.matcher(pathname.getName()).matches();
            }
        });
        Set<String> paths = new HashSet<String>();
        for (File file : files) {
            StringBuilder sb = new StringBuilder(s1);
            //System.out.println(file.getName());
            //sb.append(file.getName()).append(s2).append(g2);
            sb.append(file.getName()).append(s2);
            if(new File(sb.toString()).exists()){
                sb.append(g2);
                paths.add(sb.toString());
            }else{
            }

        }
        return paths;
    }
}
