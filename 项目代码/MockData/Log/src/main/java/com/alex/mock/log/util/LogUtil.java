// Decompiled by Jad v1.5.8g. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.kpdus.com/jad.html
// Decompiler options: packimports(3) 
// Source File Name:   LogUtil.java

package com.alex.mock.log.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtil
{

    public LogUtil()
    {
    }

    public static void log(String logString)
    {
        log.info(logString);
    }

    private static final Logger log = LoggerFactory.getLogger(LogUtil.class);

}
