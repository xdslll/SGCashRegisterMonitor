package com.sg.cash.monitor;

import com.sg.cash.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

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

    private LogFileHandler logFileHandler;
    private static final Log logger = LogFactory.getLog(LogDirHandlerTest.class);

    @Before
    public void prepare() {
        String OsName = System.getProperty("os.name");
        if (OsName.matches("^Mac.*")) {
            LogUtil.info(logger, "macos system");
            String inPath = "/Users/apple/Desktop/JKreport";
            String outPath = "/Users/apple/Desktop/JKreport2";
            logFileHandler = new LogFileHandler(inPath, outPath);
        } else {
            LogUtil.info(logger, "windows system");
            String inPath = "E:/Project/SgCashRegisterMonitor/JKreport";
            String outPath = "E:/Project/SgCashRegisterMonitor/JKreport2";
            logFileHandler = new LogFileHandler(inPath, outPath);
        }
    }

    @Test
    public void test1CopyFile() throws IOException {
        //logFileHandler.copyLogFile();
    }

    @Test
    public void test2PartitionFile() throws IOException {
        //logFileHandler.partitionFile();
    }

    @Test
    public void test3CheckFile() throws IOException {
        //logFileHandler.checkFile();
        logFileHandler.checkFileAmount();
    }

    @After
    public void clean() throws FileNotFoundException {
        //FileUtil.cleanOutputFile(logFileHandler.getLogFileOutPath());
    }
}
