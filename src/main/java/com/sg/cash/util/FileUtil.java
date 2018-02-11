package com.sg.cash.util;

import com.sg.cash.exception.FileLoadException;
import com.sg.cash.monitor.FileRunnable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 文件工具类
 *
 * @author xdslll
 * @date 2018/2/8
 **/
public class FileUtil {

    private static final Log logger = LogFactory.getLog(FileUtil.class);

    /**
     * 清空文件夹
     *
     * @param outDir
     * @throws FileNotFoundException
     */
    public static void cleanOutputFile(File outDir) throws FileNotFoundException {
        if (outDir.exists() && outDir.isDirectory()) {
            File[] outFileList = outDir.listFiles();
            if (outFileList == null) {
                return;
            } else if (outFileList.length == 0) {
                outDir.delete();
            } else {
                for (File file : outFileList) {
                    cleanOutputFile(file);
                }
                File[] newOutFileList = outDir.listFiles();
                if (newOutFileList != null && newOutFileList.length == 0) {
                    outDir.delete();
                }
            }
        } else if (outDir.exists() && outDir.isFile()) {
            outDir.delete();
        } else {
            //throw new FileNotFoundException("unknown file!");
        }
    }

    /**
     * 清空文件夹
     *
     * @param logFileOutPath
     * @throws FileNotFoundException
     */
    public static void cleanOutputFile(String logFileOutPath) throws FileNotFoundException {
        cleanOutputFile(new File(logFileOutPath));
    }

    /**
     * 获取文件名
     *
     * @param file
     * @return
     */
    public static String getFileName(File file) {
        String filePath = file.getAbsolutePath();
        filePath = filePath.replaceAll("\\\\", "/");
        if (filePath.contains("/")) {
            int index = filePath.lastIndexOf("/");
            return filePath.substring(index + 1, filePath.length());
        }
        return null;
    }

    /**
     * 剪切文件
     *
     * @param oldFile   原文件路径
     * @param newFile   新文件路径
     * @param ifCover   是否覆盖
     * @throws IOException
     */
    public static void moveFile(File oldFile, File newFile, boolean ifCover, boolean ifMove) throws IOException {
        if (!oldFile.exists() || !oldFile.isFile()) {
            return;
        }
        if (ifCover && newFile.exists() && newFile.isFile()) {
            newFile.delete();
        }
        FileInputStream fis = new FileInputStream(oldFile);
        byte[] buffer = new byte[1024 * 10];
        int hasRead;
        FileOutputStream fos = new FileOutputStream(newFile);
        while ((hasRead = fis.read(buffer, 0, buffer.length)) != -1) {
            fos.write(buffer,0, hasRead);
        }
        fos.flush();
        fis.close();
        fos.close();
        //如果是剪切则删除老文件
        if (ifMove) {
            oldFile.delete();
        }
    }

    public static void moveFile(File oldFile, File newFile) throws IOException {
        moveFile(oldFile, newFile, true, true);
    }

    public static void moveFile(String oldFilePath, String newFilePath) throws IOException {
        moveFile(new File(oldFilePath), new File(newFilePath));
    }

    public static void moveFile(String oldDirPath, String newDirPath, String fileName) throws IOException {
        moveFile(new File(oldDirPath, fileName), new File(newDirPath, fileName));
    }

    public static void copyFile(File oldFile, File newFile) throws IOException {
        moveFile(oldFile, newFile, true, false);
    }

    /**
     * 将输入文件夹下的文件拷贝至输出文件夹并转换编码格式
     * @throws FileNotFoundException
     */
    public static void copyLogFileAndConvertToUTF8(File inDir, File outDir) throws IOException {
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
     * 对文件夹进行迭代，支持通过自定义方法处理文件
     *
     * @param checkFile
     * @param originalDir
     * @param r
     * @throws IOException
     */
    public static void checkFile(File checkFile, File originalDir, FileRunnable r) throws IOException {
        //判断是否为文件夹
        if (checkFile != null && checkFile.isDirectory()) {
            //列出文件夹下的所有文件
            File[] fileList = checkFile.listFiles();
            //如果文件不存在，直接退出
            if (fileList == null || fileList.length == 0) {
                return;
            }
            //循环检查文件
            for (File subFile : fileList) {
                checkFile(subFile, originalDir, r);
            }
        }
        //判断是否为文件
        else if (checkFile != null && checkFile.isFile()) {
            //获取文件夹
            String fileName = FileUtil.getFileName(checkFile);
            if (fileName == null) {
                return;
            }
            //在源文件夹下获取同名文件
            File originalFile = new File(originalDir, fileName);
            //判断源文件是否存在
            if (!originalFile.exists()) {
                return;
            }
            //重命名变量，更易理解
            r.check(originalFile, checkFile);
        } else {
            throw new FileNotFoundException("unknown file!");
        }
    }

    /**
     * 拷贝文件夹及文件夹下的所有文件
     *
     * @param originalDir     源文件夹
     * @param targetDir       新文件夹
     */
    public static void copyDir(File originalDir, File targetDir) {
        if (!originalDir.exists()) {
            return;
        }
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        copyDirInternal(originalDir, targetDir);
    }

    /**
     * 复制文件夹
     *
     * @param dir
     * @param targetDir
     */
    public static void copyDirInternal(File dir, File targetDir) {
        File[] fileList = dir.listFiles();
        for (File file : fileList) {
            if (file.isDirectory()) {
                String dirName = file.getName();
                File destDir = new File(targetDir, dirName + File.separator);
                if (!destDir.exists()) {
                    destDir.mkdirs();
                }
                copyDirInternal(file, destDir);
            }
        }
    }

    /**
     * 判断2个文件夹是否完全一致
     *
     * @param rootDir1
     * @param rootDir2
     */
    public static boolean checkDirEqual(File rootDir1, File rootDir2) {
        if (!rootDir1.isDirectory() || !rootDir2.isDirectory()) {
            return false;
        }
        //列出所有文件
        File[] fileArray1 = rootDir1.listFiles();
        File[] fileArray2 = rootDir2.listFiles();
        if (fileArray1 == null || fileArray2 == null) {
            return false;
        }
        //排除文件，只取文件夹
        List<File> fileList1 = pickUpDirectories(fileArray1);
        List<File> fileList2 = pickUpDirectories(fileArray2);
        //比对文件夹数量
        if (fileList1.size() != fileList2.size()) {
            LogUtil.info(logger, "dir1[" + rootDir1.getAbsolutePath() + "] size=" + fileList1.size() +
                    " != dir2[" + rootDir2.getAbsolutePath() + "] size=" + fileList2.size());
            return false;
        }
        //比对文件夹名称
        for (int i = 0; i < fileList1.size(); i++) {
            File dir1 = fileList1.get(i);
            File dir2 = fileList2.get(i);
            if (!dir1.getName().equals(dir2.getName())) {
                LogUtil.info(logger, "dir1[" + dir1.getAbsolutePath() +
                        "] != dir2[" + dir2.getAbsolutePath() + "]");
                return false;
            }
            if (!checkDirEqual(dir1, dir2)) {
                return false;
            }
        }
        return true;
    }

    private static List<File> pickUpDirectories(File[] fileArray) {
        ArrayList<File> fileList = new ArrayList<>(Arrays.asList(fileArray));
        Iterator<File> it = fileList.iterator();
        while (it.hasNext()) {
            if (it.next().isFile()) {
                it.remove();
            }
        }
        return fileList;
    }
}
