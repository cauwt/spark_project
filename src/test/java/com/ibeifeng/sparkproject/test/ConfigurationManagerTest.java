package com.ibeifeng.sparkproject.test;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;

/**
 * Created by zkpk on 11/4/17.
 */
public class ConfigurationManagerTest {
    public static void main(String[] args){
        String testkey1 = ConfigurationManager.getProperties("testkey1");
        String testkey2 = ConfigurationManager.getProperties("testkey2");

        System.out.println(testkey1);
        System.out.println(testkey2);
    }
}
