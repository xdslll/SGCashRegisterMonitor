package com.sg.cash.local;

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

    public LocalLogFileHandler(File inDir, File outDir) {
        this.inDir = inDir;
        this.outDir = outDir;
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
                } else if (newFile.length() == 0) {
                    // 如果新文件的容量为0，说明文件拷贝有误，重新操作一遍
                    System.out.println("新文件[" + newFile.getAbsolutePath() + "]容量为0,删除后重新拷贝");
                    if (newFile.delete()) {
                        FileUtil.copyFileOnce(oldFile, newFile);
                        index++;
                    } else {
                        System.out.println("新文件[" + newFile.getAbsolutePath() + "]删除失败，请检查");
                    }
                }
            } else {
                FileUtil.copyFileOnce(oldFile, newFile);
                index++;
            }
        }
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
        //LogUtil.info(logger, "original file count:" + originalFileCount + ",new file count:" + newFileCount.get());
        //LogUtil.info(logger, "count test:\t" + (originalFileCount == newFileCount.get() ? "pass" : "failed"));
        System.out.print("[" + inDir.getAbsolutePath() + "] count:" + originalFileCount + ",[" + outDir.getAbsolutePath() + "] count:" + newFileCount.get());
        System.out.print("\t" + (originalFileCount == newFileCount.get() ? "pass\n" : "failed\n"));
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

    /**
     * 完成文件复制和编码转换工作
     *
     * @param localPath - 输入文件夹
     * @param localPath2 - 输出文件夹
     * @param check - 是否校验文件行数
     * @return
     */
    public static boolean convert(String localPath, String localPath2, boolean check) {
        try {
            // 生成输入文件夹总路径对象
            File dir1 = new File(localPath);
            // 获取输入文件夹下的所有子文件夹
            File[] subDirList1 = dir1.listFiles();
            // 如果文件夹不存在直接退出
            if (subDirList1 == null || subDirList1.length == 0) {
                return true;
            }
            for (int i = 0; i < subDirList1.length; i++) {
                // 如果不是文件夹直接退出本次循环
                if (subDirList1[i].isFile()) {
                    continue;
                }
                // 获取输入子文件夹对象
                File inDir = subDirList1[i];
                // 获取输出子文件夹对象
                File outDir = new File(FileUtil.appendDir(localPath2, inDir.getName()));
                // 获取转码文件处理对象
                LocalLogFileHandler localLogFileHandler = new LocalLogFileHandler(inDir, outDir);
                // 将本地数据进行复制并转码(gbk->utf8)
                localLogFileHandler.copyLogFileAndConvertToUTF8();
                // 检查文件夹下的文件数量是否一致
                localLogFileHandler.checkFileAmount();
                if (check) {
                    // 检查文件的行数是否一致
                    localLogFileHandler.checkFileLine();
                }
            }
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public static void check(String localPath) {
        System.out.println("开始检查文件夹[" + localPath + "]...");
        File dir = new File(localPath);
        if (!dir.exists()) {
            System.out.println("文件夹[" + localPath + "]不存在");
            return;
        }
        if (!dir.isDirectory()) {
            System.out.println("文件[" + localPath + "]不是文件夹");
            return;
        }
        System.out.println("文件夹总数:" + count(dir, "dir"));
        System.out.println("文件总数:" + count(dir, "file"));
        System.out.println("容量为0文件总数:" + count(dir, "zero"));
    }

    /**
     * 统计文件或文件夹数量
     *
     * @param file - 文件路径
     * @param key - dir:统计文件夹数量,file:统计文件数量,zero:统计容量为0的文件数
     * @return
     */
    public static int count(File file, String key) {
        int count = 0;
        File[] files = file.listFiles();
        if (files == null || files.length == 0) {
            return count;
        }
        for (File f : files) {
            if (f.isDirectory()) {
                if (key.equals("dir")) {
                    count++;
                }
                count += count(f, key);
            } else if (f.isFile()) {
                if (key.equals("file")) {
                    // && !f.getName().startsWith(".") && f.getName().toLowerCase().endsWith(".log")) {
                    count++;
                } else if (key.equals("zero") && f.length() == 0) {
                    // && !f.getName().startsWith(".")) {
                    // System.out.println("文件[" + f.getAbsolutePath() + "]容量为" + f.length());
                    count++;
                }
            }
        }
        return count;
    }
}
