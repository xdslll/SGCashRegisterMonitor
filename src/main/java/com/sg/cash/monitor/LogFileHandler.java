package com.sg.cash.monitor;

import com.sg.cash.exception.FileLoadException;
import com.sg.cash.model.LogDir;
import com.sg.cash.util.FileUtil;
import com.sg.cash.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

/**
 * 处理日志文件，包括：
 * 1. 编码格式转换
 * 2. 文件状态检查
 * 3. 文件格式检查
 *
 * @author xdslll
 * @date 2018/2/8
 **/
public class LogFileHandler {

    private static final Log logger = LogFactory.getLog(LogFileHandler.class);
    /**
     * 日志输入的文件夹
     */
    private String logFileInPath;

    /**
     * 日志输出的文件夹
     */
    private String logFileOutPath;

    public LogFileHandler(String logFileInPath, String logFileOutPath) {
        this.logFileInPath = logFileInPath;
        this.logFileOutPath = logFileOutPath;
    }

    /**
     * 将输入文件夹下的文件拷贝至输出文件夹（编码格式转换）
     * @throws FileNotFoundException
     */
    public void copyLogFile() throws IOException {
        File inDir = new File(logFileInPath);
        File outDir = new File(logFileOutPath);
        //如果输入文件夹不存在，直接抛异常
        if (!inDir.exists()) {
            throw new FileNotFoundException("input path is not exist!");
        }
        //如果输出文件夹不存在，创建
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        //清空原有的输出文件
        FileUtil.cleanOutputFile(outDir);
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

    /**
     * 检验文件是否存在并且是否一致
     */
    public void checkFile() throws IOException {
        File inDir = new File(logFileInPath);
        File outDir = new File(logFileOutPath);
        if (!inDir.exists() || !outDir.exists()) {
            throw new FileNotFoundException();
        }
        if (inDir.listFiles() == null || outDir.listFiles() == null) {
            throw new FileNotFoundException();
        }
        int passCount = 0;
        int failedCount = 0;
        for (File inFile : inDir.listFiles()) {
            for (File outFile : outDir.listFiles()) {
                if (checkFileLine(inFile, outFile)) {
                    passCount++;
                    break;
                } else {
                    failedCount++;
                }
            }
        }
        LogUtil.info(logger, "pass count:" + passCount + ",failed count:" + failedCount);
    }

    private boolean checkFileLine(File inFile, File outFile) throws IOException {
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
        checkFile(outDir);
    }

    /**
     * 通过迭代方法检查文件的准确性
     *
     * @param file
     * @throws IOException
     */
    public void checkFile(File file) throws IOException {
        //判断是否为文件夹
        if (file != null && file.isDirectory()) {
            //列出文件夹下的所有文件
            File[] fileList = file.listFiles();
            //如果文件不存在，直接退出
            if (fileList == null || fileList.length == 0) {
                return;
            }
            //循环检查文件
            for (File subFile : fileList) {
                checkFile(subFile);
            }
        }
        //判断是否为文件
        else if (file != null && file.isFile()) {
            //获取文件夹
            String fileName = FileUtil.getFileName(file);
            //在源文件夹下获取同名文件
            File originalFile = new File(logFileInPath, fileName);
            //判断源文件是否存在
            if (!originalFile.exists()) {
                return;
            }
            //重命名变量，更易理解
            File newFile = file;
            //检查两个文件的行数是否一致
            if (checkFileLine(originalFile, newFile)) {
                System.out.println("file[" + originalFile + "]\tpass");
            }
        } else {
            throw new FileNotFoundException("unknown file!");
        }
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
            FileInputStream fis = new FileInputStream(file);
            byte[] buffer = new byte[1024 * 10];
            FileOutputStream fos = new FileOutputStream(newFile);
            while (fis.read(buffer) != -1) {
                fos.write(buffer);
            }
            fos.flush();
            fis.close();
            fos.close();
            //删除老文件
            oldFile.delete();
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
            //System.out.println(logDir);
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
