package com.sg.cash.monitor;

import com.sg.cash.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * SAY SOMETHING ABOUT THE CLASS...
 *
 * @author xdslll
 * @date 2018/2/8
 **/
public class LogFileHandlerTest {

    private LogFileHandler logFileHandler;
    private static final Log logger = LogFactory.getLog(LogFileHandlerTest.class);

    @Before
    public void prepare() {
        String inPath = "E:/Project/SgCashRegisterMonitor/JKreport";
        String outPath = "E:/Project/SgCashRegisterMonitor/JKreport2";
        logFileHandler = new LogFileHandler(inPath, outPath);
    }

    @Test
    public void testCopyFile() throws IOException {
        //logFileHandler.copyLogFile();
    }

    @Test
    public void testCheckFile() throws IOException {
        logFileHandler.checkFile();
    }
    @After
    public void clean() throws FileNotFoundException {
        //FileUtil.cleanOutputFile(logFileHandler.getLogFileOutPath());
    }
}
