package com.cauwt.sparkproject.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Created by zkpk on 11/5/17.
 */
public class FastjsonTest {
    public static void main(String[] args) {
        String json = "[{'name':'zhangsan',age : 24},{'name':'lisi',age : 25}]";
        JSONArray jsonArray = JSONArray.parseArray(json);
        JSONObject jsonObject = jsonArray.getJSONObject(0);
        System.out.println(jsonObject.getString("name"));

    }
}
