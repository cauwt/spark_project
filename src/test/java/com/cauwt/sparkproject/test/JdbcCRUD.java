package com.cauwt.sparkproject.test;

import java.sql.*;

/**
 * Created by zkpk on 11/4/17.
 */
public class JdbcCRUD {

    @SuppressWarnings("unused")
    public static void main(String[] args) {
        //insert();
        //update();
        //detele();
        //select();
        preparedStatement();
    }

    private static void preparedStatement() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        //for select, we need to define ResultSet
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //url: main_protocol:sub_protocol://server_name:port/database
            conn = DriverManager.getConnection("jdbc:mysql://192.168.171.1:3306/spark_project?characterEncoding=utf8"
                    ,"root","root");
            //initialize statement
            String sql = "insert into test_user(name,age) values (?,?);";
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1,"对象");
            pstmt.setInt(2,23);
            int rtn = pstmt.executeUpdate();

            System.out.println("affected rows: "+ rtn);


        } catch ( Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    private static void select() {
        Connection conn = null;
        Statement stmt = null;
        //for select, we need to define ResultSet
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //url: main_protocol:sub_protocol://server_name:port/database
            conn = DriverManager.getConnection("jdbc:mysql://192.168.171.1:3306/spark_project"
                    ,"root","root");
            //initialize statement
            stmt = conn.createStatement();
            String sql = "SELECT name, age FROM test_user;";
            rs = stmt.executeQuery(sql);
            while(rs.next()){
                String name = rs.getString("name");
                int age = rs.getInt("age");
                System.out.println("name = "+ name + ",age="+age);

            }

            //System.out.println("affected rows: "+ rtn);

        }catch (Exception e){

        }
        finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e2){
                e2.printStackTrace();
            }

        }
    }

    private static void detele() {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //url: main_protocol:sub_protocol://server_name:port/database
            conn = DriverManager.getConnection("jdbc:mysql://192.168.171.1:3306/spark_project"
                    ,"root","root");
            //initialize statement
            stmt = conn.createStatement();
            String sql = "delete from test_user where name = 'wangwu';";
            int rtn = stmt.executeUpdate(sql);

            System.out.println("affected rows: "+ rtn);

        }catch (Exception e){

        }
        finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e){
                e.printStackTrace();
            }

        }

    }

    /**
     * test updating
     */
    private static void update() {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //url: main_protocol:sub_protocol://server_name:port/database
            conn = DriverManager.getConnection("jdbc:mysql://192.168.171.1:3306/spark_project"
                    ,"root","root");
            //initialize statement
            stmt = conn.createStatement();
            String sql = "update test_user set age = age+1 where name = 'wangwu';";
            int rtn = stmt.executeUpdate(sql);

            System.out.println("affected rows: "+ rtn);

        }catch (Exception e){

        }
        finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e){
                e.printStackTrace();
            }

        }

    }

    private static void insert() {
        // define connection
        //must reference Connection under java.sql instead of that under com.mysql.jdbc
        Connection conn = null;
        // define statement object
        Statement stmt = null;
        try {
            //step 1, load database driver
            //the following is a kind of java reflection.
            Class.forName("com.mysql.jdbc.Driver");
            //url: main_protocol:sub_protocol://server_name:port/database
            conn = DriverManager.getConnection("jdbc:mysql://192.168.171.1:3306/spark_project"
            ,"root","root");
            //initialize statement
            stmt = conn.createStatement();
            String sql = "insert into test_user(name,age) values ('wangwu',26);";
            int rtn = stmt.executeUpdate(sql);

            System.out.println("affected rows: "+ rtn);


        } catch ( Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e){
                e.printStackTrace();
            }

        }
    }
}
