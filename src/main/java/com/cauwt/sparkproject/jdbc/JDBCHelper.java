package com.cauwt.sparkproject.jdbc;

import com.cauwt.sparkproject.conf.ConfigurationManager;
import com.cauwt.sparkproject.constant.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by zkpk on 11/5/17.
 */
public class JDBCHelper {
    static {
        try {
            String driver = ConfigurationManager.getProperties(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private static JDBCHelper instance = null;

    private LinkedList<Connection> datasource = new LinkedList<>();
    private JDBCHelper(){
        //1. get connection pool size
        int datasourceSize = ConfigurationManager.getInteger(
                Constants.JDBC_DATASOURCE_SIZE);
        //2. create connection pool

        for(int i =0; i< datasourceSize; i++){
            String jdbcUrl = ConfigurationManager.getProperties(Constants.JDBC_URL);
            String jdbcUser = ConfigurationManager.getProperties(Constants.JDBC_USER);
            String jdbcPassword = ConfigurationManager.getProperties(Constants.JDBC_PASSWORD);
            try {
                Connection conn = DriverManager.getConnection(jdbcUrl,jdbcUser,jdbcPassword);
                datasource.push(conn);

            } catch (Exception e){
                e.printStackTrace();
            }
        }

    }
    public static JDBCHelper getInstance(){
        if (instance == null){
            synchronized (JDBCHelper.class){
                if(instance == null){
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    public synchronized Connection getConnection(){
        while(datasource.size() ==0){
            try{
                Thread.sleep(1000);
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    public synchronized void returnConnection(Connection conn){
        datasource.push(conn);
    }

    // insert,update ,delete

    /**
     *
     * @param sql
     * @param params
     * @return affected rows
     */
    public int executeUpdate(String sql, Object[] params){
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try{
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for(int i = 0; i < params.length; i++){
                pstmt.setObject(i+1,params[i]);

            }
            rtn = pstmt.executeUpdate();
        }catch  (Exception e){
            e.printStackTrace();
        }finally {
            if(conn != null){
                returnConnection(conn);
            }
        }
        return rtn;
    }

    public void executeQuery(String sql, Object[] params, QueryCallback callback){
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try{
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for(int i = 0; i < params.length; i++){
                pstmt.setObject(i+1,params[0]);

            }
            rs = pstmt.executeQuery();
            callback.process(rs);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(conn != null){
                returnConnection(conn);
            }
        }
    }

    /**
     * execute batch sql
     * @param sql
     * @param paramsList
     * @return
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList){
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try{
            conn = getConnection();
            conn.setAutoCommit(false);//cancel auto commit
            pstmt = conn.prepareStatement(sql);
            //add batch
            for(Object[] params: paramsList){
                for(int i = 0; i < params.length; i++){
                    pstmt.setObject(i+1,params[i]);
                }
                pstmt.addBatch();

            }
            //call executeBatch
            rtn = pstmt.executeBatch();
            //commit
            conn.commit();

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(conn != null){
                returnConnection(conn);
            }
        }

        return rtn;
    }

    public static interface QueryCallback{
        /**
         * process query result
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }
}
