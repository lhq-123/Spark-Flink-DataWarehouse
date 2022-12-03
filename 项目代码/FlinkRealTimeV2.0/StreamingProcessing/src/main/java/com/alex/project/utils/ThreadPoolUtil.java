package com.alex.project.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,
                            100,
                            TimeUnit.MINUTES,
                            new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }
}
