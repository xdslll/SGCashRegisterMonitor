package com.sg.cash.util;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * 文件工具类
 *
 * @author xdslll
 * @date 2018/2/8
 **/
public class FileUtil {

    /**
     * 清空文件夹
     *
     * @param outDir
     * @throws FileNotFoundException
     */
    public static void cleanOutputFile(File outDir) throws FileNotFoundException {
        if (outDir.exists() && outDir.isDirectory()) {
            File[] outFileList = outDir.listFiles();
            for (File file : outFileList) {
                if (file.exists()) {
                    if (!file.delete()) {
                        throw new FileNotFoundException("delete output file failed!");
                    }
                }
            }
        }
    }

    /**
     * 清空文件夹
     *
     * @param logFileOutPath
     * @throws FileNotFoundException
     */
    public static void cleanOutputFile(String logFileOutPath) throws FileNotFoundException {
        cleanOutputFile(new File(logFileOutPath));
    }

    /**
     * 获取文件名
     *
     * @param file
     * @return
     */
    public static String getFileName(File file) {
        String filePath = file.getAbsolutePath();
        filePath = filePath.replaceAll("\\\\", "/");
        if (filePath.contains("/")) {
            int index = filePath.lastIndexOf("/");
            return filePath.substring(index + 1, filePath.length());
        }
        return null;
    }
}
