package com.alex.mock.db.util;

import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@AllArgsConstructor
@Builder(builderClassName ="Builder" )
public class RandomOptionGroup<T> {

    int totalWeight=0;

    List<RanOpt> optList=new ArrayList();

    public static<T> Builder<T> builder(){
        return  new RandomOptionGroup.Builder<T>();
    }

    public static  class Builder<T>{
        List<RanOpt> optList=new ArrayList();

        int totalWeight=0;

        public Builder add(T value,int weight){
            RanOpt ranOpt = new RanOpt(value, weight);
            totalWeight+=weight;
            for (int i = 0; i <weight ; i++) {
                optList.add(ranOpt);
            }
            return this;
        }

        public  RandomOptionGroup<T> build(){
           return   new   RandomOptionGroup<T>(totalWeight,optList);
        }

    }

    public RandomOptionGroup(String... values){
        for (String value : values) {
            totalWeight+=1;
             optList.add(new RanOpt(value,1));
        }
    }



    public   RandomOptionGroup(RanOpt<T>... opts) {
        for (RanOpt opt : opts) {
            totalWeight+=opt.getWeight();
            for (int i = 0; i <opt.getWeight() ; i++) {
                optList.add(opt);
            }

        }
    }

   /* public   RandomOptionGroup(RanOpt<Boolean>... opts) {
        for (RanOpt opt : opts) {
            totalWeight+=opt.getWeight();
            for (int i = 0; i <opt.getWeight() ; i++) {
                optList.add(opt);
            }

        }
    }*/

    public   RandomOptionGroup(int trueWeight ,int falseWeight){
        this (new RanOpt (true, trueWeight),new RanOpt (false, falseWeight));

    }

    public  T  getValue() {
        int i = new Random().nextInt(totalWeight);
        return (T)optList.get(i).getValue();
    }

    public RanOpt<T> getRandomOpt() {
        int i = new Random().nextInt(totalWeight);
        return optList.get(i);
    }

    public String  getRandStringValue() {
        int i = new Random().nextInt(totalWeight);
        return  (String)optList.get(i).getValue();
    }

    public Integer  getRandIntValue() {
        int i = new Random().nextInt(totalWeight);
        return  (Integer)optList.get(i).getValue();
    }

    public Boolean  getRandBoolValue() {

        int i = new Random().nextInt(totalWeight);
        return  (Boolean)optList.get(i).getValue();
    }
}

