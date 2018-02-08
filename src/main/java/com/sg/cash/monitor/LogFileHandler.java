package com.sg.cash.monitor;

import com.sg.cash.exception.FileLoadException;
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
        if (!inDir.exists())
            throw new FileNotFoundException("input path is not exist!");
        //如果输出文件夹不存在，创建
        if (!outDir.exists())
            outDir.mkdirs();
        //清空原有的输出文件
        FileUtil.cleanOutputFile(outDir);
        //判断源文件不能为空
        File[] inFileList = inDir.listFiles();
        if (inFileList == null || inFileList.length == 0)
            return;
        //源文件数量
        int index = 0;
        //拷贝文件
        for (File inFile : inFileList) {
            String fileName = FileUtil.getFileName(inFile);
            FileInputStream fis = new FileInputStream(inFile);
            FileOutputStream fos = new FileOutputStream(new File(outDir, fileName));
            byte[] buffer = new byte[(int) inFile.length()];
            int hasRead = fis.read(buffer);
            if (hasRead != inFile.length())
                throw new FileLoadException("file cannot load once...");
            fos.write(buffer);
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
        if (!inDir.exists() || !outDir.exists())
            throw new FileNotFoundException();
        if (inDir.listFiles() == null || outDir.listFiles() == null)
            throw new FileNotFoundException();
        int passCount = 0;
        int failedCount = 0;
        for (File inFile : inDir.listFiles()) {
            for (File outFile : outDir.listFiles()) {
                String inFileName = FileUtil.getFileName(inFile);
                String outFileName = FileUtil.getFileName(outFile);
                if (!inFileName.equals(outFileName))
                    continue;
                BufferedReader inBr = new BufferedReader(new FileReader(inFile));
                BufferedReader outBr = new BufferedReader(new FileReader(outFile));
                int inLine = 0;
                int outLine = 0;
                while (inBr.readLine() != null) inLine++;
                while (outBr.readLine() != null) outLine++;
                if (inLine == outLine) {
                    passCount++;
                    //LogUtil.info(logger, "pass: file[" + inFileName + "] line(" + inLine + ") equals");
                } else {
                    failedCount++;
                    //LogUtil.info(logger, "failed: file[" + inFileName + "] inline=" + inLine + ",outline=" + outLine);
                }
                inBr.close();
                outBr.close();
                break;
            }
        }
        LogUtil.info(logger, "pass count:" + passCount + ",failed count:" + failedCount);
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
