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
        //localLogFileHandler.checkFileAmount();
        //localLogFileHandler.checkFileLine();
    }

    @Test
    public void test2PartitionFile() throws IOException {
        //localLogFileHandler.copyLogFileAndConvertToUTF8();
        //localLogFileHandler.partitionFile();
        //localLogFileHandler.checkFileAmount();
        //localLogFileHandler.checkFileLine();
    }

    @Test
    public void test3CheckFile() throws IOException {
        //localLogFileHandler.checkFile();
        //localLogFileHandler.checkFileAmount();
    }

    @After
    public void clean() throws FileNotFoundException {
        //FileUtil.cleanOutputFile(localLogFileHandler.getLogFileOutPath());
    }


}
