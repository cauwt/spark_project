package com.ibeifeng.sparkproject.test;

/**
 * Created by zkpk on 11/14/17.
 */
public class EqualityTest {
    public static void main(String[] args) {
        String a = new String("abc");
        String b = new String("abc");
        System.out.println(a==b);//false
        System.out.println(a.equals(b));//true

        String c = "abc";
        String d = "abc";
        System.out.println(c==d);//true
        System.out.println(c.equals(d));//true
        System.out.println(a==c);//false
        System.out.println(a.equals(d));//true

        Integer i = new Integer(1);
        Integer j = new Integer(1);
        Integer k = 1;
        System.out.println(i==j);//false
        System.out.println(i==k);//false
        System.out.println(j==k);//false
        System.out.println(i.equals(j));//true
        System.out.println(i.equals(k));//true
        System.out.println(j.equals(k));//true

    }
}
