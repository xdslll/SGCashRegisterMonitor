package com.sg.cash.local;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xiads
 * @date 2018/3/23
 * @since
 */
public class LogFileCount {

    public static final String LOCAL_FILE_PATH = "/Users/apple/Desktop/JKreport2";
    //public static final String LOCAL_FILE_PATH = "/Users/apple/Desktop/JKreport3";

    public static void main(String[] args) throws IOException {
        long _START = System.currentTimeMillis();
        doLogFileCount();
        //doLogFileLineCount();
        long _END = System.currentTimeMillis();
        System.out.println("elapsed time:" + (double) (_END - _START) / 1000 + "s");
    }

    private static void doLogFileLineCount() throws IOException {
        File[] fileList = new File(LOCAL_FILE_PATH).listFiles();
        int lineCount = 0;
        for (File file : fileList) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            BufferedReader br = new BufferedReader(new FileReader(file));
            while (br.readLine() != null) {
                lineCount++;
            }
            br.close();
        }
        System.out.println("line count:" + lineCount);
    }

    private static void doLogFileCount() throws IOException {
        File[] fileList = new File(LOCAL_FILE_PATH).listFiles();
        Map<String, Integer> resultMap = new HashMap<>();
        for (File file : fileList) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            int index = 0;
            while ((line = br.readLine()) != null) {
                ++index;
                if (!line.contains("|")) {
                    System.out.println("文件[" + file.getName() + "]第" + index + "行内容有误：" + line);
                    continue;
                }
                String[] valueArray = line.split("\\|");
                if (valueArray.length < 6) {
                    System.out.println("文件[" + file.getName() + "]第" + index + "行内容有误：" + line);
                    continue;
                }
                String name = valueArray[5];
                if (resultMap.containsKey(name)) {
                    int count = resultMap.get(name);
                    count++;
                    resultMap.put(name, count);
                } else {
                    int count = 1;
                    resultMap.put(name, count);
                }
            }
            br.close();
        }
        System.out.println(resultMap.toString());
        System.out.println(resultMap.size());
    }

}
