package com.sg.cash.hadoop.hdfs;

import com.sg.cash.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiads
 * @date 2018/3/22
 * @since
 */
public class HdfsUtil {

    public static final String HDFS_URI = "hdfs://www.mac-bigdata-01.com:8020";
    public static final String USER = "hadoop";
    public static final String HDFS_REPORT_DIR = "/sg/report/";

    /**
     * 收银日志相关的文件
     */
    public static final String HDFS_UPLOAD_DIR = "/sg/report/input/";
    public static final String HDFS_OUTPUT_DIR = "/sg/report/output/";
    public static final String LOCAL_FILE_PATH = "/Users/apple/Desktop/JKreport_2";

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        long _START = System.currentTimeMillis();
        //uploadFileToHdfs();
        //showFileCount();
        long _END = System.currentTimeMillis();
        System.out.println("elapsed time:" + (double) (_END - _START) / 1000 + "s");
    }

    private static void showFileCount() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(
                new URI(HDFS_URI),
                conf,
                USER
        );
        // create path
        Path inputDir = new Path(HDFS_UPLOAD_DIR);
        count(hdfs, inputDir);
    }

    /**
     * 上传收银机文件到hdfs
     *
     * @param hdfsRemoteUri
     * @param hdfsUser
     * @param hdfsUploadMachineDir
     * @param localCashMachineFilePath
     * @return
     */
    public static boolean uploadMachineFileToHdfs(String hdfsRemoteUri, String hdfsUser,
                                                  String hdfsUploadMachineDir, String localCashMachineFilePath) {
        FileSystem hdfs = null;
        try {
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(hdfsRemoteUri),
                    conf,
                    hdfsUser
            );
            // 对本地的门店信息文件进行数据处理
            uploadFile(hdfs, localCashMachineFilePath, hdfsUploadMachineDir);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(hdfs);
        }
        return false;
    }

    /**
     * 上传门店数据到hdfs
     *
     * @param hdfsUri
     * @param user
     * @param hdfsFileDir
     * @param localFilePath1
     * @param localFilePath2
     * @param localFilePath3
     * @return
     */
    public static boolean uploadStoreFileToHdfs(String hdfsUri, String user, String hdfsFileDir, String localFilePath1, String localFilePath2, String localFilePath3) {
        FileSystem hdfs = null;
        try {
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(hdfsUri),
                    conf,
                    user
            );
            // 对本地的门店信息文件进行数据处理
            etlStoreFile(localFilePath1, localFilePath2, localFilePath3);
            uploadFile(hdfs, localFilePath3, hdfsFileDir);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(hdfs);
        }
        return false;
    }

    /**
     * 上传日志文件到hdfs
     *
     * @param hdfsUri
     * @param user
     * @param hdfsDir
     * @param localDir
     * @param hiveWarehouse
     * @return
     */
    public static boolean uploadFileToHdfs(String hdfsUri, String user, String hdfsDir, String localDir, String hiveWarehouse) {
        FileSystem hdfs = null;
        try {
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(hdfsUri),
                    conf,
                    user
            );
            // 生成输入文件夹路径
            String hdfsInputDir = FileUtil.appendDir(hdfsDir, "input");
            Path inputDir = new Path(hdfsInputDir);

            // 清空文件夹
            //System.out.println("正在删除HDFS文件夹：" + inputDir);
            //hdfs.delete(inputDir, true);
            //System.out.println("删除HDFS文件夹：" + inputDir + "成功");
            //hdfs.delete(outputDir, true);

            // 如果文件夹不存在，则创建文件夹
            boolean mkInputDirs = hdfs.mkdirs(inputDir);
            System.out.println("创建文件夹（" + inputDir + "）：" + mkInputDirs);

            // 设置文件夹权限
            hdfs.setPermission(inputDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
            hdfs.setPermission(new Path(hdfsDir), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

            // 获取输入文件夹下的所有子文件夹
            File[] localSubInputDirs = new File(localDir).listFiles();
            if (localSubInputDirs == null || localSubInputDirs.length == 0) {
                return false;
            }
            int uploadNumber = 0;
            for (int i = 0; i < localSubInputDirs.length; i++) {
                File localSubInputDir = localSubInputDirs[i];
                // 如果是文件直接跳过
                if (localSubInputDir.isFile()) {
                    continue;
                } else if (localSubInputDir.isDirectory()) {
                    // 如果是文件夹但是以"."开头直接跳过
                    if (localSubInputDir.getName().startsWith(".")) {
                        System.out.println("文件夹[" + localSubInputDir.getAbsolutePath() + "]不符合上传条件，自动过滤");
                        continue;
                    }
                }
                // 生成hdfs子文件夹路径
                String subHdfsInputDir = FileUtil.appendDir(hdfsInputDir, localSubInputDir.getName());
                // 如果子文件夹不存在，则创建
                Path subHdfsInputDirPath = new Path(subHdfsInputDir);
                if (!hdfs.exists(subHdfsInputDirPath)) {
                    boolean mkSubInputDirs = hdfs.mkdirs(subHdfsInputDirPath);
                    System.out.println("创建子文件夹（" + subHdfsInputDirPath + "）：" + mkSubInputDirs);
                }
                // 上传日志文件
                uploadNumber += uploadDir(hdfs, localSubInputDir.getAbsolutePath(), subHdfsInputDir, hiveWarehouse);
            }
            System.out.println("上传hdfs文件总数：" + uploadNumber);
            // 校验上传文件总数
            System.out.println("hdfs文件总数:" + count(hdfs, inputDir));

            // showFiles(hdfs, inputDir);
            // showFiles(HDFS_UPLOAD_DIR);

            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(hdfs);
        }
        return false;
    }

    private static void close(FileSystem hdfs) {
        if (hdfs != null) {
            try {
                hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 指定目录下是否存在指定文件
     *
     * @param filePath - 指定文件
     * @param targetDir - 指定目录
     * @return
     */
    public static boolean existsInDirectory(FileSystem hdfs, Path filePath, String targetDir) throws IOException {
        // 判断目标文件是否存在
        if (!hdfs.exists(filePath)) {
            return false;
        }
        // 判断目标文件夹是否存在
        Path targetPath = new Path(targetDir);
        if (!hdfs.exists(targetPath)) {
            return false;
        }
        // 获取指定文件
        FileStatus file = hdfs.getFileStatus(filePath);
        // 判断是否为文件
        if (!file.isFile()) {
            return false;
        }
        // 获取指定文件夹下的所有文件
        FileStatus[] targetFileList = hdfs.listStatus(targetPath);
        // 判断目标文件夹下是否存在同样的文件
        for (FileStatus targetFile : targetFileList) {
            if (targetFile.isFile() &&
                    targetFile.getPath().getName().equals(file.getPath().getName()) &&
                    targetFile.getLen() == file.getLen()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 读取文件总数
     *
     * @param hdfs
     * @param inputDir
     * @return
     * @throws IOException
     */
    private static int count(FileSystem hdfs, Path inputDir) throws IOException {
        int count = 0;
        FileStatus[] files = hdfs.listStatus(inputDir);
        for (FileStatus file : files) {
           if (file.isDirectory()) {
               count += count(hdfs, file.getPath());
           } else if (file.isFile()) {
               count++;
           }
        }
        return count;
    }

    private static void etlStoreFile(String oldFilePath, String newFilePath, String finalFilePath) throws IOException {
        // 先将文件转为gbk格式
        File oldFile = new File(oldFilePath);
        File newFile = new File(newFilePath);
        FileInputStream fis = new FileInputStream(oldFile);
        FileOutputStream fos = new FileOutputStream(newFile);
        byte[] buffer = new byte[(int) oldFile.length()];
        int hasRead = fis.read(buffer);
        if (hasRead != oldFile.length()) {
            throw new FileNotFoundException("file cannot load once...");
        }
        fos.write(new String(buffer, "GBK").getBytes());
        fos.flush();
        fis.close();
        fos.close();

        File finalFile = new File(finalFilePath);
        PrintStream ps = new PrintStream(finalFile);
        BufferedReader br = new BufferedReader(new FileReader(newFile));
        String line;
        while ((line = br.readLine()) != null) {
            //System.out.println(line);
            StringBuilder newLine = new StringBuilder();
            String[] elements = line.split(",");
            if (elements.length == 6) {
                String storeNo = elements[0];
                if (storeNo.length() == 1) {
                    storeNo = "000" + storeNo;
                } else if (storeNo.length() == 2) {
                    storeNo = "00" + storeNo;
                } else if (storeNo.length() == 3) {
                    storeNo = "0" + storeNo;
                }
                String city = elements[4];
                String smallArea = elements[5];
                if (!smallArea.equals(city) && smallArea.contains(city)) {
                    smallArea = smallArea.replace(city, "")
                            .replace("-", "");
                }
                newLine.append(storeNo)
                        .append(",")
                        .append(elements[1])
                        .append(",")
                        .append(elements[2])
                        .append(",")
                        .append(elements[3])
                        .append(",")
                        .append(city)
                        .append(",")
                        .append(smallArea);
            } else {
                newLine.append(line);
            }
            // System.out.println(newLine);
            ps.println(newLine.toString());
            ps.flush();
        }
        br.close();
        ps.close();
    }

    /**
     * 上传单个文件
     *
     * @param hdfs
     * @param localFilePath
     */
    private static void uploadFile(FileSystem hdfs, String localFilePath, String hdfsFilePath) throws IOException {
        // 获取文件名
        int index = localFilePath.lastIndexOf("/");
        String fileName = localFilePath.substring(index + 1, localFilePath.length());
        // 生成hdfs上传文件路径
        Path path = new Path(hdfsFilePath + fileName);
        if (hdfs.exists(path)) {
            FileStatus file = hdfs.getFileStatus(path);
            if (file.getLen() == new File(localFilePath).length()) {
                System.out.println("文件[" + file.getPath() + "]已经上传,无需更新");
                return;
            }
        }
        // 设置hdfs输出流
        FSDataOutputStream out = hdfs.create(path, true);
        // 设置文件权限
        FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
        hdfs.setPermission(path, permission);
        // 生成本地文件输入流
        FileInputStream in = new FileInputStream(localFilePath);
        // 拷贝文件
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * 上传文件夹到hdfs
     *
     * @param hdfs
     * @return
     * @throws IOException
     */
    public static int uploadDir(FileSystem hdfs, String localFilePath, String hdfsInputPath, String hiveWarehouse) throws IOException {
        int uploadFileNumber = 0;
        // 获取原文件夹下的所有文件
        File[] fileList = new File(localFilePath).listFiles();
        // 如果没有文件则跳出
        if (fileList == null || fileList.length == 0) {
            return uploadFileNumber;
        }
        System.out.println("文件夹[" + localFilePath + "]文件总数:" + fileList.length);
        for (File file : fileList) {
            // 获取文件名
            String fileName = file.getName();
            // 如果文件以.开头或没有以log结尾都不处理
            if (fileName.startsWith(".") || !fileName.toLowerCase().endsWith(".log")) {
                System.out.println("文件[" + file.getName() + "]不符合上传条件，自动过滤");
                continue;
            }
            // 如果原文件容量为0，不上传
            if (file.length() == 0) {
                System.out.println("原文件[" + file.getAbsolutePath() + "]容量为0，不上传");
                continue;
            }
            // 如果文件在hdfs上已经存在，并且长度相同，则不再上传
            Path path = new Path(hdfsInputPath + fileName);
            if (hdfs.exists(path)) {
                // System.out.println("文件（" + path + "）已经存在！");
                FileStatus fileStatus = hdfs.listStatus(path)[0];
                // 比对文件容量，如果容量一致，则不上传
                if (fileStatus.getLen() == file.length()) {
                    continue;
                }
            } else {
                // 如果hive仓库中存在同样文件则不上传
                Path hiveWarehouseFilePath = new Path(new StringBuilder()
                        .append(hiveWarehouse)
                        .append(fileName)
                        .toString());
                if (hdfs.exists(hiveWarehouseFilePath)) {
                    // System.out.println("hive仓库存在文件[" + hiveWarehouseFilePath + "]");
                    // 如果文件的容量与原文件的容量不一致，则需要上传
                    FileStatus hiveWarehouseFile = hdfs.getFileStatus(hiveWarehouseFilePath);
                    if (hiveWarehouseFile.isFile() && hiveWarehouseFile.getLen() == file.length()) {
                        continue;
                    } else {
                        // 如果容量不一致，先删除hive仓库中的该文件
                        hdfs.delete(hiveWarehouseFilePath, true);
                        System.out.println("hive仓库文件[" + hiveWarehouseFilePath + "]与原文件不一致，已删除");
                    }
                }
            }
            // 上传文件
            System.out.println("开始上传文件[" + localFilePath + "]->[" + hdfsInputPath + "]");
            // 创建hdfs文件输出流
            FSDataOutputStream out = hdfs.create(path, true);
            // 设置文件权限
            FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            hdfs.setPermission(path, permission);
            // 读取原文件
            FileInputStream in = new FileInputStream(file.getAbsolutePath());
            // 拷贝文件到指定目录
            IOUtils.copyBytes(in, out, 4096, true);
            System.out.println("上传第" + (uploadFileNumber + 1) + "个文件[" + file.getName() + "]成功,文件容量=" + file.length());
            uploadFileNumber++;
        }
        return uploadFileNumber;
    }

    public static void showFiles(String localFilePath) {
        showFiles(new File(localFilePath));
    }

    public static void showFiles(File dir) {
        if (dir == null) {
            return;
        }
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                System.out.println(">>>" + files[i].getPath());
                // 递归调用
                showFiles(files[i]);
            } else if (files[i].isFile()) {
                System.out.println("   " + files[i].getPath()
                        + ", length:" + files[i].length()
                        + ", modified:" + files[i].lastModified());
            }
        }
    }

    /**
     * @param hdfs FileSystem 对象
     * @param path 文件路径
     */
    public static void showFiles(FileSystem hdfs, Path path) {
        try {
            if (hdfs == null || path == null || !hdfs.exists(path)) {
                return;
            }
            // 获取文件列表
            FileStatus[] files = hdfs.listStatus(path);
            System.out.println("文件总数：" + files.length);
            // 展示文件信息
            for (int i = 0; i < files.length; i++) {
                try {
                    if (files[i].isDirectory()) {
                        System.out.println(">>>" + files[i].getPath()
                                + ", dir owner:" + files[i].getOwner());
                        // 递归调用
                        showFiles(hdfs, files[i].getPath());
                    } else if (files[i].isFile()) {
                        System.out.println("   " + files[i].getPath()
                                + ", length:" + files[i].getLen()
                                + ", owner:" + files[i].getOwner()
                                + ", modified:" + files[i].getModificationTime());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 比对两个目录及下属文件是否一致
     *
     * @param hdfs
     * @param dir1
     * @param dir2
     * @return
     */
    public static boolean compareDirectories(FileSystem hdfs, String dir1, String dir2) throws IOException {
        Path path1 = new Path(dir1);
        if (!hdfs.exists(path1)) {
            return false;
        }
        Path path2 = new Path(dir2);
        if (!hdfs.exists(path2)) {
            return false;
        }
        FileStatus[] files1 = hdfs.listStatus(path1);
        FileStatus[] files2 = hdfs.listStatus(path2);
        if (files1 == null || files1.length == 0 ||
                files2 == null || files2.length == 0) {
            return false;
        }
        System.out.println("[" + path1 + "] length=" + files1.length + ",[" + path2 + "] length=" + files2.length);
        int totalNumber = files1.length;
        int sameNumber = 0;
        for (int i = 0; i < files1.length; i++) {
            FileStatus file1 = files1[i];
            boolean same = false;
            for (int j = 0; j < files2.length; j++) {
                FileStatus file2 = files2[i];
                if (equals(file1, file2)) {
                    //System.out.println("[" + file1 + "]与[" + file2 + "]相同");
                    sameNumber++;
                    same = true;
                    break;
                }
            }
            if (!same) {
                System.out.println("发现不同文件[" + file1.getPath() + "]");
            }
        }
        System.out.println("文件总数:" + totalNumber);
        System.out.println("相同文件总数:" + sameNumber);
        return totalNumber == sameNumber;
    }

    public static boolean equals(FileStatus file1, FileStatus file2) {
        return file1.getPath().getName().equals(file2.getPath().getName()) &&
                file1.getLen() == file2.getLen();
    }

    public static boolean nameEquals(FileStatus file1, FileStatus file2) {
        return file1.getPath().getName().equals(file2.getPath().getName());
    }

    public static boolean compareDirectories(String hdfsUri, String user, String dir1, String dir2) {
        FileSystem hdfs = null;
        try {
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(hdfsUri),
                    conf,
                    user
            );
            return compareDirectories(hdfs, dir1, dir2);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(hdfs);
        }
        return false;
    }

    public static boolean upload(String hdfsUri, String user, String hdfsPath, String localPath) {
        FileSystem hdfs = null;
        try {
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(hdfsUri),
                    conf,
                    user
            );
            Path p = new Path(hdfsPath);
            if (!hdfs.exists(p)) {
                return false;
            }
            FSDataOutputStream out = hdfs.create(p, true);
            InputStream in = new FileInputStream(localPath);
            IOUtils.copyBytes(in, out, 4096, true);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(hdfs);
        }
        return false;
    }

    public static boolean download(String hdfsUri, String user, String hdfsPath, String localPath) {
        FileSystem hdfs = null;
        try {
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(hdfsUri),
                    conf,
                    user
            );
            Path p = new Path(hdfsPath);
            if (!hdfs.exists(p)) {
                return false;
            }
            FSDataInputStream in = hdfs.open(p);
            OutputStream out = new FileOutputStream(localPath);
            IOUtils.copyBytes(in, out, 4096, true);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(hdfs);
        }
        return false;
    }

    public static void check(String hdfsRemoteUri, String hdfsUser, String hdfsDir) {
        FileSystem hdfs = null;
        try {
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(hdfsRemoteUri),
                    conf,
                    hdfsUser
            );
            Path p = new Path(hdfsDir);
            if (!hdfs.exists(p)) {
                System.out.println("文件夹[" + p.toString() + "]不存在");
                return;
            }
            System.out.println("文件夹[" + p.toString() + "]下文件夹数量:" + count(hdfs, p, "dir"));
            System.out.println("文件夹[" + p.toString() + "]下文件数量:" + count(hdfs, p, "file"));
            System.out.println("文件夹[" + p.toString() + "]下容量为0文件数量:" + count(hdfs, p, "zero"));
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(hdfs);
        }
    }

    private static int count(FileSystem hdfs, Path p, String key) throws IOException {
        int count = 0;
        FileStatus[] files = hdfs.listStatus(p);
        for (FileStatus file : files) {
            if (file.isDirectory()) {
                if (key.equals("dir")) {
                    count++;
                }
                count += count(hdfs, file.getPath(), key);
            } else if (file.isFile()) {
                if (key.equals("file")) {
                    count++;
                } else if (key.equals("zero")) {
                    if (file.getLen() == 0) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    /**
     * 检查被激活的hdfs连接
     * @param hdfsRemoteUri
     * @param hdfsRemoteUri2
     * @param hdfsUser
     * @return
     */
    public static String checkActiveHdfs(String hdfsRemoteUri, String hdfsRemoteUri2, String hdfsUser) {
        FileSystem hdfs = null;
        FileSystem hdfs2 = null;
        try {
            hdfs = createHdfs(hdfsRemoteUri, hdfsUser);
            hdfs2 = createHdfs(hdfsRemoteUri2, hdfsUser);
            Path p = new Path("/");
            try {
                if (hdfs.exists(p)) {
                    return hdfsRemoteUri;
                }
            } catch(Exception ex) {
                // ex.printStackTrace();
                System.out.println(hdfsRemoteUri + " 处于standby状态");
            }
            try {
                if (hdfs2.exists(p)) {
                    return hdfsRemoteUri2;
                }
            } catch(Exception ex) {
                // ex.printStackTrace();
                System.out.println(hdfsRemoteUri2 + " 处于standby状态");
            }
        } catch (Exception ex) {
            //ex.printStackTrace();
            System.out.println(ex.getMessage());
        } finally {
            close(hdfs);
            close(hdfs2);
        }
        return null;
    }

    private static FileSystem createHdfs(String hdfsRemoteUri, String hdfsUser) throws URISyntaxException, IOException, InterruptedException {
        // 生成配置文件
        Configuration conf = new Configuration();
        // 生成hdfs对象
        return FileSystem.get(
                new URI(hdfsRemoteUri),
                conf,
                hdfsUser
        );
    }

    public static void clearDuplicated(String hdfsRemoteUri, String hdfsUser, String hdfsDir) {
        FileSystem hdfs = null;
        List<Path> deleteFiles = new ArrayList<>();
        try {
            hdfs = createHdfs(hdfsRemoteUri, hdfsUser);
            Path p = new Path(hdfsDir);
            if (!hdfs.exists(p)) {
                return;
            }
            FileStatus[] dirs = hdfs.listStatus(p);
            for (FileStatus dir : dirs) {
                if (dir.isDirectory()) {
                    System.out.println("正在扫描文件夹[" + dir.getPath() + "]");
                    FileStatus[] files = hdfs.listStatus(dir.getPath());
                    for (FileStatus file : files) {
                        if (file.isFile()) {
                            for (FileStatus file2: files) {
                                Path p1 = file.getPath();
                                Path p2 = file2.getPath();
                                String fileName1 = getFileName(p1);
                                String fileName2 = getFileName(p2);
                                //System.out.println(fileName1 + "," + fileName2);
                                if (!fileName1.equals(fileName2) &&
                                        fileName1.toLowerCase().equals(fileName2.toLowerCase())) {
                                    System.out.println("发现重复文件:" + p1.getName() + "," + p2.getName());
                                    System.out.println(file.getModificationTime() + "," + file2.getModificationTime());
                                    if (file.getModificationTime() > file2.getModificationTime()) {
                                        //System.out.println("应删除[" + p2.getName() + "]");
                                        deleteFiles.add(p2);
                                    } else {
                                        //System.out.println("应删除[" + p1.getName() + "]");
                                        deleteFiles.add(p1);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for (Path deleteFile : deleteFiles) {
                if (hdfs.exists(deleteFile)) {
                    System.out.println("正在删除文件[" + deleteFile.getName() + "]...");
                    /*if (hdfs.delete(deleteFile, true)) {
                        System.out.println("删除文件[" + deleteFile.getName() + "]成功");
                    } else {
                        System.out.println("删除文件[" + deleteFile.getName() + "]失败");
                    }*/
                } else {
                    System.out.println("文件[" + deleteFile + "]不存在");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(hdfs);
        }
    }

    private static String getFileName(Path p) {
        String path = p.getName();
        int index = path.lastIndexOf("/");
        return path.substring(index + 1);
    }
}
