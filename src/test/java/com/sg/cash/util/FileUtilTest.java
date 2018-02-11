package com.sg.cash.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author xiads
 * @date 11/02/2018
 * @since
 */
public class FileUtilTest {

    private static final Log logger = LogFactory.getLog(FileUtilTest.class);

    private void copyOrMoveFile() throws IOException {
        //复制/剪切文件
        String oldDirPath = "/Users/apple/Desktop/11/";
        String newDirPath = "/Users/apple/Desktop/22/";
        String fileName = "0185_0002_20180201.log";
        File oldFile = new File(oldDirPath, fileName);
        File newFile = new File(newDirPath, fileName);
        long oldFileLength = oldFile.length();
        //FileUtil.moveFile(oldFile, newFile);
        FileUtil.copyFile(oldFile, newFile);
        long newFileLength = newFile.length();
        //检查结果
        Assert.assertTrue(oldFile.exists());
        Assert.assertTrue(newFile.exists());
        LogUtil.info(logger, "oldFileLength=" + oldFileLength + ",newFileLength=" + newFileLength);
        Assert.assertTrue(oldFileLength == newFileLength);
    }

    @Test
    public void copyOrMoveFileTest() throws IOException {
        //copyOrMoveFile();
    }

    private void copyDir() {
        String originalDirPath = "/Users/apple/Desktop/JKreport2";
        String targetDirPath = "/Users/apple/Desktop/JKreport3";
        File originalDir = new File(originalDirPath);
        File targetDir = new File(targetDirPath);
        FileUtil.copyDir(originalDir, targetDir);
    }

    @Test
    public void copyDirTest() {
        //copyDir();
    }

    private void prepareTestData() throws IOException {
        String testPath[] = {"/Users/apple/Desktop/2/1/",
                //"/Users/apple/Desktop/2/1/1/",
                //"/Users/apple/Desktop/2/1/1",
                "/Users/apple/Desktop/2/2/",
                "/Users/apple/Desktop/2/3/",
                "/Users/apple/Desktop/3/1/",
                "/Users/apple/Desktop/3/2/",
                "/Users/apple/Desktop/3/3/"};
        for (int i = 0; i < testPath.length; i++) {
            String path = testPath[i];
            if (path.endsWith(File.separator)) {
                File dir = new File(path);
                if (!dir.exists()) {
                    dir.mkdirs();
                }
            } else {
                File file = new File(path);
                if (!file.exists()) {
                    file.createNewFile();
                }
            }
        }
    }

    private void checkDirEqual() throws IOException {
        //准备测试数据
        prepareTestData();
        //测试文件夹比对
        //String originalDirPath = "/Users/apple/Desktop/2";
        //String targetDirPath = "/Users/apple/Desktop/3";
        String originalDirPath = "/Users/apple/Desktop/JKreport2";
        String targetDirPath = "/Users/apple/Desktop/JKreport3";
        File originalDir = new File(originalDirPath);
        File targetDir = new File(targetDirPath);
        //检查输出结果
        Assert.assertTrue(FileUtil.checkDirEqual(originalDir, targetDir));
    }

    @Test
    public void checkDirEqualTest() throws IOException {
        checkDirEqual();
    }
}
