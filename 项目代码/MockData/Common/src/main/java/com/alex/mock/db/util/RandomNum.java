package com.alex.mock.db.util;

import java.util.Random;

public class RandomNum {
    public static final  int getRandInt(int fromNum,int toNum){

        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }



    public static final  int getRandInt(int fromNum,int toNum,Long seed){

        return   fromNum+ new Random(seed).nextInt(toNum-fromNum+1);
    }
}

