package com.sg.cash.monitor;

import com.sg.cash.exception.FileLoadException;
import com.sg.cash.model.LogDir;
import com.sg.cash.util.FileUtil;
import com.sg.cash.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 处理日志文件，包括：
 * 1. 编码格式转换
 * 2. 文件状态检查
 * 3. 文件格式检查
 *
 * @author xdslll
 * @date 2018/2/8
 **/
public class LocalLogFileHandler {

    private static final Log logger = LogFactory.getLog(LocalLogFileHandler.class);
    /**
     * 日志输入的文件夹
     */
    private String logFileInPath;

    /**
     * 日志输出的文件夹
     */
    private String logFileOutPath;

    public LocalLogFileHandler(String logFileInPath, String logFileOutPath) {
        this.logFileInPath = logFileInPath;
        this.logFileOutPath = logFileOutPath;
    }

    public void copyLogFileAndConvertToUTF8() throws IOException {
        File inDir = new File(logFileInPath);
        File outDir = new File(logFileOutPath);
        //如果输入文件夹不存在，直接抛异常
        if (!inDir.exists()) {
            throw new FileNotFoundException("input path is not exist!");
        }
        //清空原有的输出文件
        FileUtil.cleanOutputFile(outDir);
        //如果输出文件夹不存在，创建
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        //FileUtil.copyDir(inDir, outDir);
        //判断源文件不能为空
        File[] inFileList = inDir.listFiles();
        if (inFileList == null || inFileList.length == 0) {
            return;
        }
        //源文件数量
        int index = 0;
        //拷贝文件
        for (File inFile : inFileList) {
            String fileName = FileUtil.getFileName(inFile);
            FileInputStream fis = new FileInputStream(inFile);
            FileOutputStream fos = new FileOutputStream(new File(outDir, fileName));
            byte[] buffer = new byte[(int) inFile.length()];
            int hasRead = fis.read(buffer);
            if (hasRead != inFile.length()) {
                throw new FileLoadException("file cannot load once...");
            }
            fos.write(new String(buffer, "GBK").getBytes());
            fos.flush();
            fis.close();
            fos.close();
            index++;
            LogUtil.debug(logger, "file[" + fileName + "] is copied successfully.");

        }
        LogUtil.info(logger, "index=" + index);
    }

    protected boolean checkFileLine(File inFile, File outFile) throws IOException {
        BufferedReader inBr = null;
        BufferedReader outBr = null;
        try {
            String inFileName = FileUtil.getFileName(inFile);
            String outFileName = FileUtil.getFileName(outFile);
            if (!inFileName.equals(outFileName)) {
                return false;
            }
            inBr = new BufferedReader(new FileReader(inFile));
            outBr = new BufferedReader(new FileReader(outFile));
            int inLine = 0;
            int outLine = 0;
            while (inBr.readLine() != null) {
                inLine++;
            }
            while (outBr.readLine() != null) {
                outLine++;
            }
            System.out.print("file[" + inFileName + "]\tinline=" + inLine + ",outline=" + outLine);
            if (inLine == outLine) {
                return true;
            } else {
                return false;
            }
        } finally {
            if (inBr != null) {
                inBr.close();
            }
            if (outBr != null) {
                outBr.close();
            }
        }
    }

    public void checkFileAmount() throws IOException {
        File outDir = new File(logFileOutPath);
        File inDir = new File(logFileInPath);
        //检查文件数量是否一致
        int originalFileCount = new File(logFileInPath).listFiles().length;
        AtomicInteger newFileCount = new AtomicInteger();
        FileUtil.checkFile(outDir, inDir, (originalFile, newFile) -> newFileCount.addAndGet(1));
        LogUtil.info(logger, "original file count:" + originalFileCount + ",new file count:" + newFileCount.get());
        LogUtil.info(logger, "count test:\t" + (originalFileCount == newFileCount.get() ? "pass" : "failed"));
    }

    public void checkFileLine() throws IOException {
        File outDir = new File(logFileOutPath);
        File inDir = new File(logFileInPath);
        AtomicInteger pass = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        FileUtil.checkFile(outDir, inDir, (originalFile, newFile) -> {
            //检查剪切后的文件与源文件的行数是否一致
            if (checkFileLine(originalFile, newFile)) {
                System.out.print("\tpass\n");
                pass.addAndGet(1);
            } else {
                System.out.print("\tfailed\n");
                failed.addAndGet(1);
            }
        });
        LogUtil.info(logger, "line test:pass=" + pass.get() + ",failed=" + failed.get());
    }

    /**
     * 对文件进行分区
     */
    public void partitionFile() throws IOException {
        File outDir = new File(logFileOutPath);
        File[] outFileList = outDir.listFiles();
        if (outFileList == null || outFileList.length == 0) {
            return;
        }
        //分区逻辑：年/月/店号/工号
        for (File file : outFileList) {
            String fileName = FileUtil.getFileName(file);
            if (fileName == null || !fileName.contains("_")) {
                continue;
            }
            //根据文件名，找出年份、店号和工号，生成文件夹
            LogDir newDir = convertFileNameToDir(fileName);
            newDir.mkdir(outDir);
            File oldFile = file;
            File newFile = new File(newDir.getDir(), fileName);
            //将文件移动至新文件夹下
            FileUtil.moveFile(oldFile, newFile);
        }
    }

    /**
     * 将文件名转换为文件夹
     *
     * @param fileName
     * @return
     */
    public LogDir convertFileNameToDir(String fileName) {
        LogDir logDir = new LogDir();
        String[] section = fileName.split("_");
        if (section.length >= 3) {
            String storeCode = section[0];
            String empCode = section[1];
            String yearAndMonth = section[2].substring(0, section[2].lastIndexOf("."));
            String sYear = yearAndMonth.substring(0, 4);
            String sMonth = yearAndMonth.substring(4, 6);
            logDir.setEmpCode(empCode);
            logDir.setStoreCode(storeCode);
            logDir.setMonth(sMonth);
            logDir.setYear(sYear);
        }
        return logDir;
    }

    public String getLogFileInPath() {
        return logFileInPath;
    }

    public void setLogFileInPath(String logFileInPath) {
        this.logFileInPath = logFileInPath;
    }

    public String getLogFileOutPath() {
        return logFileOutPath;
    }

    public void setLogFileOutPath(String logFileOutPath) {
        this.logFileOutPath = logFileOutPath;
    }
}
