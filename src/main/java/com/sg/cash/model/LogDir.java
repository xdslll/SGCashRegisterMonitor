package com.sg.cash.model;

import java.io.File;

/**
 * 日志文件的存储文件夹
 *
 * @author xiads
 * @date 09/02/2018
 * @since
 */
public class LogDir {

    private String year;
    private String month;
    private String storeCode;
    private String empCode;
    private File dir;

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getStoreCode() {
        return storeCode;
    }

    public void setStoreCode(String storeCode) {
        this.storeCode = storeCode;
    }

    public String getEmpCode() {
        return empCode;
    }

    public void setEmpCode(String empCode) {
        this.empCode = empCode;
    }

    public boolean mkdir(File outDir) {
        StringBuilder dirPath = new StringBuilder();
        if (year != null && month != null && storeCode != null && empCode != null) {
            dirPath.append(year)
                    .append(File.separator)
                    .append(month)
                    .append(File.separator)
                    .append(storeCode)
                    .append(File.separator)
                    .append(empCode)
                    .append(File.separator);
            File dir = new File(outDir, dirPath.toString());
            this.dir = dir;
            if (!dir.exists()) {
                return dir.mkdirs();
            }
        }
        return false;
    }

    public File getDir() {
        return dir;
    }

    @Override
    public String toString() {
        return "LogDir{" +
                "year='" + year + '\'' +
                ", month='" + month + '\'' +
                ", storeCode='" + storeCode + '\'' +
                ", empCode='" + empCode + '\'' +
                ", dir=" + dir +
                '}';
    }
}
