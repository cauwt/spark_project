package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.domain.Top10Category;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.io.Serializable;

/**
 * Created by zkpk on 11/14/17.
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

    @Override
    public void insert(Top10Category top10Category) {
        String sql = "INSERT INTO top10_category (" +
                "task_id" +
                ", category_id" +
                ", click_count" +
                ", order_count" +
                ", pay_count) " +
                "VALUES (?, ?, ?, ?, ?);";
        Object[] params = new Object[]{top10Category.getTaskId()
                ,top10Category.getCategoryId()
                ,top10Category.getClickCount()
                ,top10Category.getOrderCount()
                ,top10Category.getPayCount()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
