package com.sg.cash.hadoop.ftp;

import com.sg.cash.util.ConfigUtil;
import com.sg.cash.util.FileUtil;
import org.apache.commons.net.ftp.*;

import java.io.*;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiads
 * @date 2018/4/15
 * @since
 */
public class FtpUtil {

    public static final String HOSTNAME = ConfigUtil.get("ftp_host");
    public static final int PORT = ConfigUtil.getInt("ftp_port");
    public static final String USERNAME = ConfigUtil.get("ftp_username");
    public static final String PASSWORD = ConfigUtil.get("ftp_password");

    public FTPClient ftpClient = null;

    public FtpUtil() {}

    public FTPClient getFtpClient() {
        return ftpClient;
    }

    /**
     * 连接ftp服务器
     */
    public void connect() throws SocketException {
        ftpClient = new FTPClient();
        ftpClient.setControlEncoding("UTF-8");
        ftpClient.setDataTimeout(600 * 1000);
        ftpClient.setConnectTimeout(600 * 1000);
        ftpClient.setControlKeepAliveReplyTimeout(600 * 1000);
        ftpClient.setControlKeepAliveTimeout(600);
        ftpClient.setDefaultTimeout(600 * 1000);
        try {
            System.out.println("开始连接ftp站点");
            System.out.println("地址：" + HOSTNAME);
            System.out.println("端口号：" + PORT);
            System.out.println("用户名：" + USERNAME);
            System.out.println("密码：" + PASSWORD);

            ftpClient.connect(HOSTNAME, PORT);
            ftpClient.login(USERNAME, PASSWORD);
            int replyCode = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(replyCode)) {
                System.out.println("连接失败");
                ftpClient.setSoTimeout(600 * 1000);
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
                    deleteLocalFile(localFile);
                    flag = doDownload(ftpFileName, localFile);
                } else {
                    // System.out.println("本地文件[" + localFile.getAbsolutePath() + "]存在，无需更新");
                    // 如果本地文件的容量为0，则删除后重新下载
                    // 如果本地文件与ftp文件容量的差值大于1KB，说明文件没有同步完成，删除后重新下载
                    if (localFile.exists() && (localFile.length() == 0 ||
                            Math.abs(localFile.length() - ftpFile.getSize()) > 1024)) {
                        deleteLocalFile(localFile);
                        flag = doDownload(ftpFileName, localFile);
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return flag;
    }

    private void deleteLocalFile(File localFile) {
        if (localFile.exists() && localFile.delete()) {
            System.out.println("本地文件[" + localFile.getAbsolutePath() + "]删除成功");
        } else {
            System.out.println("本地文件[" + localFile.getAbsolutePath() + "]删除失败或文件不存在");
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
            ftpClient.enterLocalPassiveMode();
            String[] ftpFileNames = ftpClient.listNames(ftpPath);
            // 如果文件不存在，直接退出
            if (ftpFileNames == null || ftpFileNames.length == 0) {
                return false;
            }
            // ftp累计删除文件总数
            long ftpDeleteFileNumber = 0;
            for (String ftpFilePath : ftpFileNames) {
                // 如果不是以.log结尾，说明是文件夹，不处理
                if (!ftpFilePath.toLowerCase().endsWith(".log")) {
                    continue;
                }
                // 获取文件的FTPFile对象
                ftpClient.enterLocalPassiveMode();
                FTPFile ftpFile = ftpClient.listFiles(ftpFilePath)[0];
                // 如果不是文件，则不处理
                if (ftpFile == null || !ftpFile.isFile()) {
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
                    String originalFilePath = new StringBuilder()
                            .append(ftpPath)
                            .append(ftpFileName)
                            .toString();
                    // 移动后文件的绝对路径
                    String dateFilePath = new StringBuilder()
                            .append(datePath)
                            .append(ftpFileName)
                            .toString();
                    // 判断日期文件夹是否存在
                    if (ftpClient.changeWorkingDirectory(datePath)) {
                        System.out.println("文件夹[" + datePath + "]存在!");
                        if (ftpFile.isFile() && ftpFile.isValid() && !ftpFile.isUnknown()) {
                            // 定义是否需要移动文件
                            boolean needMove = true;
                            // 定义日期文件夹中是否包含了同样的文件
                            boolean containFile = false;
                            // 比对日期目录下是否已经包含了根目录下的文件，防止文件重复
                            ftpClient.enterLocalPassiveMode();
                            FTPFile[] targetFileList = ftpClient.listFiles(datePath);
                            for (FTPFile targetFile : targetFileList) {
                                if (targetFile.getName().toLowerCase().equals(ftpFileName.toLowerCase())) {
                                    if (ftpFile.getSize() == 0) {
                                        // 如果原文件容量为0，直接删除新文件，并且不做移动操作
                                        ftpUtil.delete(originalFilePath);
                                        ftpDeleteFileNumber++;
                                        needMove = false;
                                    } else if (ftpFile.getSize() > 0 && targetFile.getSize() == ftpFile.getSize()) {
                                        // 如果容量一致，直接删除新文件，并且不做移动操作
                                        ftpUtil.delete(originalFilePath);
                                        ftpDeleteFileNumber++;
                                        needMove = false;
                                    } else {
                                        needMove = true;
                                    }
                                    // 如果文件名一致，说明包含了同样的文件
                                    containFile = true;
                                    break;
                                }
                            }
                            // 移动文件
                            if (needMove) {
                                // 如果日期文件夹下存在同名文件，则先删除
                                if (containFile) {
                                    ftpUtil.delete(dateFilePath);
                                    ftpDeleteFileNumber++;
                                }
                                System.out.println("开始移动[" + originalFilePath + "]->[" + dateFilePath + "]");
                                // 将文件移动到指定目录下
                                ftpUtil.move(originalFilePath, dateFilePath);
                            } else {
                                System.out.println("[" + originalFilePath + "]->[" + dateFilePath + "]一致，无需复制");
                            }
                        } else if (ftpFile.isDirectory()) {
                            System.out.println("文件[" + ftpFileName + "]是文件夹!");
                        } else {
                            System.out.println("文件[" + ftpFileName + "]状态异常!");
                        }
                    } else {
                        System.out.println("文件夹[" + datePath + "]不存在!");
                        // 如果日期文件夹不存在，则创建
                        if (ftpClient.makeDirectory(datePath)) {
                            System.out.println("文件夹[" + datePath + "]创建成功!");
                            // 将文件移动到指定目录下
                            ftpUtil.move(originalFilePath, dateFilePath);
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
                System.out.println("ftp文件夹[" + ftpPath + ftpDir.getName() + "]下文件总数:" + ftpDirFiles.length);
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

    public static void checkZero(String ftpPath) {
        FtpUtil ftpUtil = new FtpUtil();
        try {
            ftpUtil.connect();
            FTPClient ftpClient = ftpUtil.getFtpClient();
            // 统计是否存在容量为0的文件
            System.out.println("容量为0文件总数:" + zero(ftpPath, ftpClient));
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            ftpUtil.destroyFtpClient();
        }
    }

    public static void check(String ftpPath) {
        FtpUtil ftpUtil = new FtpUtil();
        try {
            ftpUtil.connect();
            FTPClient ftpClient = ftpUtil.getFtpClient();
            System.out.println("autodetect utf8:" + ftpClient.getAutodetectUTF8());
            System.out.println("charset:" + ftpClient.getCharset());
            System.out.println("local address:" + ftpClient.getLocalAddress().toString());
            System.out.println("local port:" + ftpClient.getLocalPort());
            System.out.println("remote address:" + ftpClient.getRemoteAddress().toString());
            System.out.println("remote port:" + ftpClient.getRemotePort());
            System.out.println("status:" + ftpClient.getStatus());
            System.out.println("system type:" + ftpClient.getSystemType());
            System.out.println("tcp no delay:" + ftpClient.getTcpNoDelay());
            System.out.println("isAvailable:" + ftpClient.isAvailable());
            System.out.println("isConnected:" + ftpClient.isConnected());
            System.out.println("default timeout:" + ftpClient.getDefaultTimeout());
            System.out.println("connect timeout:" + ftpClient.getConnectTimeout());
            System.out.println("control keep alive timeout:" + ftpClient.getControlKeepAliveTimeout());
            System.out.println("control keep alive reply timeout:" + ftpClient.getControlKeepAliveReplyTimeout());
            System.out.println("so timeout:" + ftpClient.getSoTimeout());
            System.out.println("ftp path:" + ftpPath);

            // System.out.println("changeWorkingDirectory(" + ftpPath + ")" + ftpUtil.changeWorkingDirectory(ftpPath));
            // System.out.println("list:" + ftpClient.list());
            // System.out.println(ftpClient.listFiles(ftpPath).length);
            // System.out.println(ftpClient.listNames(ftpPath).length);
            // 统计文件夹总数
            ftpClient.enterLocalPassiveMode();
            FTPFile[] dirs = ftpClient.listDirectories(ftpPath);
            System.out.println("文件夹总数:" + dirs.length);
            // 统计文件总数
            System.out.println("文件总数:" + count(ftpPath, ftpClient));
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            ftpUtil.destroyFtpClient();
        }
    }

    private static int zero(String ftpPath, FTPClient ftpClient) throws IOException {
        ftpClient.enterLocalPassiveMode();
        int count = 0;
        String[] ftpFileNames = ftpClient.listNames(ftpPath);
        if (ftpFileNames == null || ftpFileNames.length == 0) {
            return count;
        }
        for (String ftpFilePath : ftpFileNames) {
            // System.out.println("正在比对文件[" + ftpFilePath + "]");
            if (ftpFilePath.toLowerCase().endsWith(".log")) {
                ftpClient.enterLocalPassiveMode();
                FTPFile ftpFile = ftpClient.listFiles(ftpFilePath)[0];
                if (ftpFile.getSize() == 0) {
                    count++;
                }
            } else {
                count += zero(ftpFilePath, ftpClient);
            }
        }
        return count;
    }

    public static int count(String ftpPath, FTPClient ftpClient) throws IOException {
        int count = 0;
        ftpClient.enterLocalPassiveMode();
        String[] ftpFileNames = ftpClient.listNames(ftpPath);
        if (ftpFileNames == null || ftpFileNames.length == 0) {
            return count;
        }
        for (String ftpFilePath : ftpFileNames) {
            //System.out.println("正在比对文件[" + ftpFilePath + "]");
            if (ftpFilePath.toLowerCase().endsWith(".log")) {
                count++;
            } else {
                count += count(ftpFilePath, ftpClient);
            }
        }
        return count;
    }

    public static void diff(String ftpPath, String localPath) {
        FtpUtil ftpUtil = new FtpUtil();
        try {
            ftpUtil.connect();
            FTPClient ftpClient = ftpUtil.getFtpClient();
            diff(ftpPath, localPath, ftpClient);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            ftpUtil.destroyFtpClient();
        }
    }

    public static void compareFtpToLocal(String[] originalFtpFiles, String[] ftpFiles, String[] localFiles) {
        if (ftpFiles != null) {
            for (int i = 0; i < originalFtpFiles.length; i++) {
                boolean hasFile = false;
                if (localFiles != null) {
                    for (String localFile : localFiles) {
                        if (ftpFiles[i].equals(localFile)) {
                            hasFile = true;
                            break;
                        }
                    }
                }
                if (!hasFile) {
                    System.out.println("[" + originalFtpFiles[i] + "] ftp: have, local: not have");
                }
            }
        }
    }

    public static void compareLocalToFtp(String[] ftpFileNames, String localPath, String[] localFiles) {
        if (localFiles != null) {
            for (int i = 0; i < localFiles.length; i++) {
                boolean hasFile = false;
                if (ftpFileNames != null) {
                    for (String ftpFile : ftpFileNames) {
                        if (localFiles[i].equals(ftpFile)) {
                            hasFile = true;
                            break;
                        }
                    }
                }
                if (!hasFile) {
                    System.out.println("[" + localPath + localFiles[i] + "] local: have, ftp: not have");
                }
            }
        }
    }

    public static void diff(String ftpPath, String localPath, FTPClient ftpClient) throws IOException {
        ftpClient.enterLocalPassiveMode();
        String[] originalFtpFileNames = ftpClient.listNames(ftpPath);
        String[] ftpFileNames = new String[originalFtpFileNames.length];
        for (int i = 0; i < ftpFileNames.length; i++) {
            int index = originalFtpFileNames[i].lastIndexOf("/");
            ftpFileNames[i] = originalFtpFileNames[i].substring(index + 1);
        }
        String[] localFiles = new File(localPath).list();
        compareFtpToLocal(originalFtpFileNames, ftpFileNames, localFiles);
        compareLocalToFtp(ftpFileNames, localPath, localFiles);

        for (String originalFtpDir : originalFtpFileNames) {
            ftpClient.enterLocalPassiveMode();
            String dirName = originalFtpDir.substring(originalFtpDir.lastIndexOf("/") + 1);
            String[] originalSubFtpFileNames = ftpClient.listNames(originalFtpDir);
            String[] ftpSubFileNames = new String[originalSubFtpFileNames.length];
            for (int i = 0; i < ftpSubFileNames.length; i++) {
                int index = originalSubFtpFileNames[i].lastIndexOf("/");
                ftpSubFileNames[i] = originalSubFtpFileNames[i].substring(index + 1);
            }
            String[] localSubFileNames = new File(localPath + dirName).list();
            compareFtpToLocal(originalSubFtpFileNames, ftpSubFileNames, localSubFileNames);
            compareLocalToFtp(ftpSubFileNames, localPath, localSubFileNames);
        }
    }
}
