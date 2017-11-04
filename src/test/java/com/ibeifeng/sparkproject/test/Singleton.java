package com.ibeifeng.sparkproject.test;

/**
 * Created by zkpk on 11/4/17.
 * private constructor
 * static initialization method: eg getInstance
 * use locker to avoid multi-thread issue
 * don't use sychronized Singleton getInstance()
 */
public class Singleton {
    private static Singleton instance= null;
    private Singleton(){

    }
    public static Singleton getInstance(){
        //double null-check
        if (instance == null){
            synchronized (Singleton.class){
                if(instance == null){
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }
}
