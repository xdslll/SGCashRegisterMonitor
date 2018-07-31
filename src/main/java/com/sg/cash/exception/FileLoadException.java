package com.sg.cash.exception;

/**
 * 文件读取异常
 *
 * @author xdslll
 * @date 2018/2/8
 **/
public class FileLoadException extends RuntimeException {

    public FileLoadException(String message) {
        super(message);
    }
}
