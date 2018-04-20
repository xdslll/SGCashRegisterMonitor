package com.sg.cash.hadoop.ftp;

import com.sg.cash.util.FileUtil;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.*;
import java.net.SocketException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiads
 * @date 2018/4/15
 * @since
 */
public class FtpUtil {

    public static final String HOSTNAME = "192.168.57.10";
    public static final int PORT = 21;
    public static final String USERNAME = "FtpUser";
    public static final String PASSWORD = "Xds840126";

    public FTPClient ftpClient = null;

    public FtpUtil() {}

    public FTPClient getFtpClient() {
        return ftpClient;
    }

    public void setFtpClient(FTPClient ftpClient) {
        this.ftpClient = ftpClient;
    }

    /**
     * 连接ftp服务器
     */
    public void connect() throws SocketException {
        ftpClient = new FTPClient();
        ftpClient.setControlEncoding("UTF-8");
        ftpClient.setDataTimeout(60 * 1000);
        ftpClient.setConnectTimeout(60 * 1000);
        ftpClient.setControlKeepAliveReplyTimeout(60 * 1000);
        ftpClient.setControlKeepAliveTimeout(60);
        ftpClient.setDefaultTimeout(60 * 1000);
        try {
            System.out.println("开始连接ftp站点");
            ftpClient.connect(HOSTNAME, PORT);
            ftpClient.login(USERNAME, PASSWORD);
            int replyCode = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(replyCode)) {
                System.out.println("连接失败");
                ftpClient.setSoTimeout(60 * 1000);
            } else {
                System.out.println("连接成功");
            }
            // 设置文件传输类型
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 从FTP服务器下载文件
     *
     * @param ftpFile - ftp文件对象
     * @param ftpPath - ftp文件绝对路径
     * @param localPath - 本地路径
     * @param needCover - 是否需要覆盖
     * @return
     */
    public boolean downloadFile(FTPFile ftpFile, String ftpPath, String localPath, boolean needCover) {
        boolean flag = false;
        try {
            // 只处理当前文件夹下的文件
            if (ftpFile.isFile()) {
                // 如果没有以分隔符结尾，加上分隔符
                if (!localPath.endsWith(File.separator)) {
                    localPath = new StringBuilder()
                            .append(localPath)
                            .append(File.separator)
                            .toString();
                }
                if (!ftpPath.endsWith(File.separator)) {
                    ftpPath = new StringBuilder()
                            .append(ftpPath)
                            .append(File.separator)
                            .toString();
                }
                // 根据ftp文件生成本地文件名
                File localFile = new File(new StringBuilder()
                        .append(localPath)
                        .append(ftpFile.getName())
                        .toString());
                // 生成ftp文件绝对路径
                String ftpFileName = new StringBuilder()
                        .append(ftpPath)
                        .append(ftpFile.getName())
                        .toString();
                // 如果本地文件不存在，则直接下载，或者文件存在但需要更新，则删除文件后下载
                if (!localFile.exists() || needCover) {
                    deleteFile(localFile);
                    flag = doDownload(ftpFileName, localFile);
                } else {
                    // System.out.println("本地文件[" + localFile.getAbsolutePath() + "]存在，无需更新");
                    // 如果本地文件的容量为0，则删除后重新下载
                    // 如果本地文件与ftp文件容量的差值大于1KB，说明文件没有同步完成，删除后重新下载
                    if (localFile.exists() && (localFile.length() == 0 ||
                            Math.abs(localFile.length() - ftpFile.getSize()) > 1024)) {
                        deleteFile(localFile);
                        flag = doDownload(ftpFileName, localFile);
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return flag;
    }

    private void deleteFile(File localFile) {
        if (localFile.exists() && localFile.delete()) {
            System.out.println("本地文件[" + localFile.getAbsolutePath() + "]删除成功");
        } else {
            System.out.println("本地文件[" + localFile.getAbsolutePath() + "]不存在");
        }
    }

    private boolean doDownload(String ftpFileName, File localFile) {
        OutputStream os = null;
        boolean flag = false;
        try {
            os = new FileOutputStream(localFile);
            System.out.println("下载文件[" + ftpFileName + "]->[" + localFile + "]");
            if (flag = ftpClient.retrieveFile(ftpFileName, os)) {
                System.out.println("文件[" + localFile + "]下载成功!");
            } else {
                System.out.println("文件[" + localFile + "]下载失败!");
            }
            os.flush();
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            close(os);
        }
        return flag;
    }

    public boolean downloadFile(FTPFile ftpFile, String ftpPath, String localPath) {
        return downloadFile(ftpFile, ftpPath, localPath, false);
    }
    /**
     * 移动文件
     *
     * @param oldFilePath
     * @param newFilePath
     * @return
     * @throws IOException
     */
    public boolean move(String oldFilePath, String newFilePath) throws IOException {
        boolean flag;
        // 将文件移动到指定目录下
        if (flag = ftpClient.rename(oldFilePath, newFilePath)) {
            System.out.println("移动[" + oldFilePath + "]->[" + newFilePath + "]成功");
        } else {
            System.out.println("移动[" + oldFilePath + "]->[" + newFilePath + "]失败");
        }
        return flag;
    }

    public void destroyFtpClient() {
        System.out.println("开始销毁FTPClient");
        if (ftpClient != null && ftpClient.isConnected()) {
            try {
                ftpClient.disconnect();
                ftpClient = null;
                System.out.println("销毁成功");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close(Closeable os) {
        if (null != os) {
            try {
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean delete(String filePath) throws IOException {
        boolean flag;
        if (flag = ftpClient.deleteFile(filePath)) {
            System.out.println("删除文件[" + filePath + "]成功");
        } else {
            System.out.println("删除文件[" + filePath + "]失败");
        }
        return flag;
    }


    public boolean changeWorkingDirectory(String path) {
        boolean flag = true;
        try {
            flag = ftpClient.changeWorkingDirectory(path);
            if (flag) {
                System.out.println("进入[" + path + "]目录成功");
            } else {
                System.out.println("进入[" + path + "]目录失败");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return flag;
    }

    public boolean uploadFile(String pathname, String fileName, String originfilename) {
        boolean flag = false;
        InputStream in = null;
        try {
            System.out.println("开始上传文件");
            in = new FileInputStream(new File(originfilename));
            connect();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            createDirectory(pathname);
            ftpClient.makeDirectory(pathname);
            ftpClient.changeWorkingDirectory(pathname);
            ftpClient.storeFile(fileName, in);
            in.close();
            ftpClient.logout();
            flag = true;
            System.out.println("上传文件成功");
        } catch (Exception e) {
            System.out.println("上传文件失败");
            e.printStackTrace();
        } finally {
            destroyFtpClient();
            close(in);
        }
        return flag;
    }

    public boolean createDirectory(String remote) throws IOException {
        boolean success = true;
        String directory = remote + "/";
        // 如果远程目录不存在，则递归创建远程服务器目录
        if (!directory.equalsIgnoreCase("/") && !changeWorkingDirectory(new String(directory))) {
            int start = 0;
            int end = 0;
            if (directory.startsWith("/")) {
                start = 1;
            } else {
                start = 0;
            }
            end = directory.indexOf("/", start);
            String path = "";
            String paths = "";
            while (true) {
                String subDirectory = new String(remote.substring(start, end).getBytes("GBK"), "iso-8859-1");
                path = path + "/" + subDirectory;
                if (!existFile(path)) {
                    if (makeDirectory(subDirectory)) {
                        changeWorkingDirectory(subDirectory);
                    } else {
                        System.out.println("创建目录[" + subDirectory + "]失败");
                        changeWorkingDirectory(subDirectory);
                    }
                } else {
                    changeWorkingDirectory(subDirectory);
                }

                paths = paths + "/" + subDirectory;
                start = end + 1;
                end = directory.indexOf("/", start);
                // 检查所有目录是否创建完毕
                if (end <= start) {
                    break;
                }
            }
        }
        return success;
    }

    public boolean existFile(String path) throws IOException {
        boolean flag = false;
        FTPFile[] ftpFileArr = ftpClient.listFiles(path);
        if (ftpFileArr.length > 0) {
            flag = true;
        }
        return flag;
    }

    public boolean makeDirectory(String dir) {
        boolean flag = true;
        try {
            flag = ftpClient.makeDirectory(dir);
            if (flag) {
                System.out.println("创建文件夹[" + dir + "]成功！");
            } else {
                System.out.println("创建文件夹[" + dir + "]失败！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 同步ftp数据到本地
     *
     * @param ftpPath
     * @param localPath
     */
    public static boolean sync(String ftpPath, String localPath) {
        FtpUtil ftpUtil = new FtpUtil();
        try {
            // 连接ftp服务器
            ftpUtil.connect();
            // 获取FTPClient
            FTPClient ftpClient = ftpUtil.getFtpClient();
            // 获取指定目录下的所有文件
            FTPFile[] ftpFiles = ftpClient.listFiles(ftpPath);
            // 如果文件不存在，直接退出
            if (ftpFiles == null || ftpFiles.length == 0) {
                return false;
            }
            // ftp累计删除文件总数
            long ftpDeleteFileNumber = 0;
            // 完成所有新日志文件的分组工作
            for (FTPFile ftpFile : ftpFiles) {
                // 如果是文件夹则直接跳过
                if (ftpFile.isDirectory()) {
                    continue;
                }
                // 获取文件名
                String ftpFileName = ftpFile.getName();
                // 通过文件名获取日志所在日期
                Matcher m = PATTERN.matcher(ftpFileName);
                if (m.find()) {
                    // 获取日期
                    String date = m.group();
                    // 生成文件路径
                    String datePath = new StringBuilder()
                            .append(ftpPath)
                            .append(date)
                            .append(File.separator)
                            .toString();
                    // 原有文件的绝对路径
                    String oldFilePath = new StringBuilder()
                            .append(ftpPath)
                            .append(ftpFileName)
                            .toString();
                    // 移动后文件的绝对路径
                    String newFilePath = new StringBuilder()
                            .append(datePath)
                            .append(ftpFileName)
                            .toString();
                    // 判断日期文件夹是否存在
                    if (ftpClient.changeWorkingDirectory(datePath)) {
                        // System.out.println("文件夹[" + datePath + "]存在!");
                        if (ftpFile.isFile() && ftpFile.isValid() && !ftpFile.isUnknown()) {
                            // 判断指定目录下是否存在新文件
                            boolean needMove = false;
                            // 比对日期目录下是否已经包含了根目录下的文件，防止文件重复
                            FTPFile[] targetFileList = ftpClient.listFiles(datePath);
                            for (FTPFile file : targetFileList) {
                                if (file.getName().equals(ftpFileName)) {
                                    // 如果文件名一致，判断容量是否一致，如果不一致，则需要更新
                                    if (file.getSize() != ftpFile.getSize()) {
                                        needMove = true;
                                    } else {
                                        //如果容量一致，则直接删除老文件，保留新文件
                                        ftpUtil.delete(oldFilePath);
                                        ftpDeleteFileNumber++;
                                    }
                                    break;
                                }
                            }
                            if (needMove) {
                                ftpUtil.delete(newFilePath);
                                ftpDeleteFileNumber++;
                                System.out.println("开始移动[" + oldFilePath + "]->[" + newFilePath + "]");
                                // 将文件移动到指定目录下
                                ftpUtil.move(oldFilePath, newFilePath);
                            }
                        } else if (ftpFile.isDirectory()) {
                            // System.out.println("文件[" + ftpFileName + "]是文件夹!");
                        } else {
                            System.out.println("文件[" + ftpFileName + "]状态异常!");
                        }
                    } else {
                        System.out.println("文件夹[" + datePath + "]不存在!");
                        // 如果日期文件夹不存在，则创建
                        if (ftpClient.makeDirectory(datePath)) {
                            System.out.println("文件夹[" + datePath + "]创建成功!");
                            // 将文件移动到指定目录下
                            ftpUtil.move(oldFilePath, newFilePath);
                        } else {
                            System.out.println("文件夹[" + datePath + "]创建失败!");
                        }
                    }
                }
            }
            // ftp文件总数
            long ftpFileNumber = 0;
            // 本地文件总数
            long localFileNumber = 0;
            // 读取所有文件夹
            FTPFile[] ftpDirs = ftpClient.listDirectories(ftpPath);
            for (FTPFile ftpDir : ftpDirs) {
                // 获取文件夹的绝对路径
                String dirPath = new StringBuilder()
                        .append(ftpPath)
                        .append(ftpDir.getName())
                        .toString();
                // 获取文件夹下的所有文件
                FTPFile[] ftpDirFiles = ftpClient.listFiles(dirPath);
                System.out.println("文件夹[" + ftpDir.getName() + "]下文件总数:" + ftpDirFiles.length);
                ftpFileNumber += ftpDirFiles.length;
                for (FTPFile file : ftpDirFiles) {
                    // 生成新文件夹名称
                    String newLocalPath = new StringBuilder()
                            .append(localPath)
                            .append(ftpDir.getName())
                            .toString();
                    File dir = new File(newLocalPath);
                    // 判断文件夹是否存在
                    if (!dir.exists()) {
                        // 如果不存在则创建文件夹
                        if (dir.mkdirs()) {
                            System.out.println("本地文件夹[" + newLocalPath + "]创建成功");
                        } else {
                            System.out.println("本地文件夹[" + newLocalPath + "]创建失败");
                        }
                    }
                    // 从ftp下载文件到本地
                    if (ftpUtil.downloadFile(file, dirPath, newLocalPath)) {
                        localFileNumber++;
                    }
                }
            }
            System.out.println("ftp文件总数:" + ftpFileNumber);
            System.out.println("本地文件总数:" + FileUtil.count(new File(localPath)));
            System.out.println("ftp累计删除文件总数:" + ftpDeleteFileNumber);
            System.out.println("本地累计更新成功总数:" + localFileNumber);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            ftpUtil.destroyFtpClient();
        }
        return false;
    }

    /**
     * 获取日期的正则表达式
     */
    public static final Pattern PATTERN = Pattern.compile("[0-9]{8}");

}
