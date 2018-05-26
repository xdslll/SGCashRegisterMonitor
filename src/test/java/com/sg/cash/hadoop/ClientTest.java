package com.sg.cash.hadoop;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author xiads
 * @date 2018/5/26
 * @since
 */
public class ClientTest {

    @Test
    public void test() {
        File dir = new File("/Users/apple/Desktop/111");
        System.out.println(dir.getAbsolutePath());
        File[] files = dir.listFiles();
        List<Date> begin = new ArrayList<>();
        List<Date> end = new ArrayList<>();
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        for (File file : files) {
            int index = 1;
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(file));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] lines = line.split("\\|");
                    if (lines.length < 10) {
                        System.out.println("正在解析文件[" + file.getAbsolutePath() + "]第" + index + "行");
                        System.out.println("lines.length=" + lines.length);
                    } else if (lines[8].equals("") || lines[9].equals("") ) {
                        System.out.println("正在解析文件[" + file.getAbsolutePath() + "]第" + index + "行");
                        System.out.println("begin=" + lines[8] + ",end=" + lines[9]);
                    } else {
                        Long _begin = Long.valueOf(lines[8]) / 1000;
                        Long _end = Long.valueOf(lines[9]) / 1000;
                        Date dBegin = df.parse(String.valueOf(_begin));
                        Date dEnd = df.parse(String.valueOf(_end));
                        begin.add(dBegin);
                        end.add(dEnd);
                        long minus = (dEnd.getTime() - dBegin.getTime()) / 1000;
                        if (minus < 0) {
                            System.out.println("正在解析文件[" + file.getAbsolutePath() + "]第" + index + "行");
                            System.out.println(minus + "s");
                        }
                    }
                    index++;
                }
            } catch(Exception ex) {
                ex.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        //System.out.println(begin.toString());
        //System.out.println(end.toString());
    }

}
