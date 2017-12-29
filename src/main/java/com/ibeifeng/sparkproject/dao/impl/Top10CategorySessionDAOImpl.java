package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.dao.ITop10CategorySessionDAO;
import com.ibeifeng.sparkproject.domain.Top10Category;
import com.ibeifeng.sparkproject.domain.Top10CategorySession;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by zkpk on 11/14/17.
 */
public class Top10CategorySessionDAOImpl implements ITop10CategorySessionDAO {

    @Override
    public void insert(Top10CategorySession top10CategorySession) {
        String sql = "INSERT INTO top10_category_Session (" +
                "task_id" +
                ", category_id" +
                ", session_id" +
                ", click_count) " +
                "VALUES (?, ?, ?, ?);";
        Object[] params = new Object[]{top10CategorySession.getTaskId()
                ,top10CategorySession.getCategoryId()
                ,top10CategorySession.getSessionId()
                ,top10CategorySession.getClickCount()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
