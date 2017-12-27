package com.ibeifeng.sparkproject.test;

import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zkpk on 11/5/17.
 */
public class JDBCHelperTest {
    public static void main(String[] args) throws Exception{
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        //test executeUpdate
        //jdbcHelper.executeUpdate("insert into test_user(name,age) values(?,?);",
        //        new Object[] {"wanger",28});

        //test executeQuery
//        final Map<String, Integer> testUser = new HashMap<>();
//
//        jdbcHelper.executeQuery("select name,age from test_user where name = ?;"
//                , new Object[]{"wanger"}
//                , (ResultSet rs) -> {
//                        while(rs.next()){
//                            String name = rs.getString(1);
//                            int age = rs.getInt(2);
//                            testUser.put(name,age);
//                        }
//                    });
//        testUser.forEach((key ,value) -> System.out.println("name="+key+", age="+value));

        //test executeBatch
        String sql = "insert into test_user(name,age) values(?,?)";
        List<Object[]> paramsList = new ArrayList<>();
        paramsList.add(new Object[]{"user1",22});
        paramsList.add(new Object[]{"user2",24});
        jdbcHelper.executeBatch(sql,paramsList);


    }

}
