package com.cauwt.sparkproject.dao.impl;

import com.cauwt.sparkproject.dao.ISessionDetailDAO;
import com.cauwt.sparkproject.domain.SessionDetail;
import com.cauwt.sparkproject.jdbc.JDBCHelper;

import java.io.Serializable;

/**
 * Created by zkpk on 11/14/17.
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO, Serializable {
    private static final long serialVersionUID = -7173213376400531735L;

    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "INSERT INTO session_detail (" +
                "task_id" +
                ", user_id" +
                ", session_id" +
                ", page_id" +
                ", action_time" +
                ", search_keyword" +
                ", click_category_id" +
                ", click_product_id" +
                ", order_category_ids" +
                ", order_product_ids" +
                ", pay_category_ids" +
                ", pay_product_ids) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);";
        Object[] params = new Object[]{sessionDetail.getTaskId()
                ,sessionDetail.getUserId()
                ,sessionDetail.getSessionId()
                ,sessionDetail.getPageId()
                ,sessionDetail.getActionTime()
                ,sessionDetail.getSearchKeyword()
                ,sessionDetail.getClickCategoryId()
                ,sessionDetail.getClickProductId()
                ,sessionDetail.getOrderCategoryIds()
                ,sessionDetail.getOrderProductIds()
                ,sessionDetail.getPayCategoryIds()
                ,sessionDetail.getPayProductIds()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
