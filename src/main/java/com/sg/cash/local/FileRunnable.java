package com.sg.cash.local;

import java.io.File;
import java.io.IOException;

public interface FileRunnable {

        void check(File originalFile, File newFile) throws IOException;
}