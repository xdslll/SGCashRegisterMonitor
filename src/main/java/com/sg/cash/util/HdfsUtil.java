package com.sg.cash.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author xiads
 * @date 26/02/2018
 * @since
 */
public class HdfsUtil {

    public static FileSystem getHdfs(String uri, String user) throws URISyntaxException, IOException, InterruptedException {
        return FileSystem.get(new URI(uri), new Configuration(), user);
    }

    public static boolean mkdir(FileSystem fs, String path) throws IOException {
        return fs.mkdirs(new Path(path));
    }

    public static boolean rmdir(FileSystem fs, String path) throws IOException {
        Path p = new Path(path);
        if (fs.isDirectory(p)) {
            return fs.delete(p, true);
        }
        return false;
    }

    public static boolean deleteFile(FileSystem fs, String path) throws IOException {
        Path p = new Path(path);
        if (fs.isFile(p)) {
            return fs.delete(p, false);
        }
        return false;
    }

    public void copyFile(FileSystem fs, String hdfsPath, String localPath) throws IOException {
        FSDataOutputStream out = fs.create(new Path(hdfsPath), true);
        FileInputStream in = new FileInputStream(localPath);
        IOUtils.copyBytes(in, out, 4096, true);
    }

    public void downloadFile(FileSystem fs, String hdfsPath, String localPath) throws IOException {
        FSDataInputStream in = fs.open(new Path(hdfsPath));
        FileOutputStream out = new FileOutputStream(localPath);
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
