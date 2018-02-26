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
    private File inDir;

    /**
     * 日志输出的文件夹路径
     */
    private File outDir;

    public LocalLogFileHandler(String logFileInPath, String logFileOutPath) {
        this.inDir = new File(logFileInPath);
        this.outDir = new File(logFileOutPath);
    }

    /**
     * 从输入文件夹拷贝所有文件到输出文件夹
     *
     * @throws IOException
     */
    public void copyLogFileAndConvertToUTF8() throws IOException {
        //如果输入文件夹不存在，直接抛异常
        if (!inDir.exists()) {
            throw new FileNotFoundException("input path is not exist!");
        }
        //清空原有的输出文件
        //FileUtil.cleanOutputFile(outDir);

        if (!outDir.exists()) {
            if (!outDir.mkdirs()) {
                throw new FileNotFoundException("create output path error!");
            }
        }
        //判断源文件不能为空
        File[] inFileList = inDir.listFiles();
        if (inFileList == null || inFileList.length == 0) {
            return;
        }
        //源文件数量
        int index = 0;
        //拷贝文件
        for (File oldFile : inFileList) {
            //获取文件名，创建新文件
            String fileName = oldFile.getName();
            File newFile = new File(outDir, fileName);
            if (newFile.exists()) {
                //判断新文件和老文件是否一致，如果一致就不需要更新，如果新文件更新，那么就更新
                long oldModified = oldFile.lastModified();
                long newModified = newFile.lastModified();
                if (oldModified > newModified) {
                    FileUtil.copyFileOnce(oldFile, newFile);
                    index++;
                }
            } else {
                FileUtil.copyFileOnce(oldFile, newFile);
                index++;
            }
        }
        LogUtil.info(logger, "index=" + index);
    }




    protected boolean checkFileLine(File inFile, File outFile) throws IOException {
        BufferedReader inBr = null;
        BufferedReader outBr = null;
        try {
            String inFileName = inFile.getName();
            String outFileName = outFile.getName();
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
        //检查文件数量是否一致
        int originalFileCount = inDir.listFiles().length;
        AtomicInteger newFileCount = new AtomicInteger();
        FileUtil.checkFile(outDir, inDir, (originalFile, newFile) -> newFileCount.addAndGet(1));
        LogUtil.info(logger, "original file count:" + originalFileCount + ",new file count:" + newFileCount.get());
        LogUtil.info(logger, "count test:\t" + (originalFileCount == newFileCount.get() ? "pass" : "failed"));
    }

    public void checkFileLine() throws IOException {
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
        File[] outFileList = outDir.listFiles();
        if (outFileList == null || outFileList.length == 0) {
            return;
        }
        //分区逻辑：年/月/店号/工号
        for (File file : outFileList) {
            String fileName = file.getName();
            if (fileName == null || !fileName.contains("_")) {
                continue;
            }
            //根据文件名，找出年份、店号和工号，生成文件夹
            LogDir newDir = convertFileNameToDir(fileName);
            newDir.mkdir(outDir);
            File oldFile = file;
            File newFile = new File(newDir.getDir(), fileName);
            //对文件进行分区，删除原文件
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

    public File getInDir() {
        return inDir;
    }

    public void setInDir(File inDir) {
        this.inDir = inDir;
    }

    public File getOutDir() {
        return outDir;
    }

    public void setOutDir(File outDir) {
        this.outDir = outDir;
    }
}
