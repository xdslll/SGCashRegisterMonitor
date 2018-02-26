package com.sg.cash.monitor;

import com.sg.cash.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.*;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * 测试日志文件处理类
 *
 * @author xdslll
 * @date 2018/2/8
 **/
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LogDirHandlerTest {

    private LocalLogFileHandler localLogFileHandler;
    private static final Log logger = LogFactory.getLog(LogDirHandlerTest.class);

    @Before
    public void prepare() {
        String OsName = System.getProperty("os.name");
        LogUtil.info(logger, OsName);
        if (OsName.matches("^Mac.*")) {
            String inPath = "/Users/apple/Desktop/JKreport";
            String outPath = "/Users/apple/Desktop/JKreport2";
            localLogFileHandler = new LocalLogFileHandler(inPath, outPath);
        } else {
            String inPath = "E:/Project/SgCashRegisterMonitor/JKreport";
            String outPath = "E:/Project/SgCashRegisterMonitor/JKreport2";
            localLogFileHandler = new LocalLogFileHandler(inPath, outPath);
        }
    }

    @Test
    public void test1CopyFile() throws IOException {
        //localLogFileHandler.copyLogFileAndConvertToUTF8();
    }

    @Test
    public void test2PartitionFile() throws IOException {
        //复制文件
        localLogFileHandler.copyLogFileAndConvertToUTF8();
        //检查两个文件夹下的文件总数量是否一致
        File inDir = localLogFileHandler.getInDir();
        File outDir = localLogFileHandler.getOutDir();
        Assert.assertNotNull(inDir);
        Assert.assertNotNull(outDir);
        Assert.assertTrue(inDir.listFiles().length == outDir.listFiles().length);

        //localLogFileHandler.partitionFile();
    }

    @Test
    public void test3CheckFile() throws IOException {
        //localLogFileHandler.checkFileAmount();
        //localLogFileHandler.checkFileLine();
    }

    @After
    public void clean() throws FileNotFoundException {
        //FileUtil.cleanOutputFile(localLogFileHandler.getLogFileOutPath());
    }


}
