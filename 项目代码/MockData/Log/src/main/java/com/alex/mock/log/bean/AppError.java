package com.alex.mock.log.bean;

import com.  alex.mock.db.util.RandomNum;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.net.ConnectException;

@Data
@AllArgsConstructor
public class AppError {

    Integer error_code;

    String msg;

    public static  AppError build(){
        int errorCode = RandomNum.getRandInt(1001, 4001);
        String msg=" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.  alex.bean.log.mock.AppError.main(AppError.java:xxxxxx)";
        return new AppError(errorCode,msg);

    }



    public static void main(String[] args) throws  Exception {
        throw  new ConnectException() ;
    }
}
