package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ISessionRandomExtractDAO;
import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by zkpk on 11/14/17.
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "INSERT INTO session_random_extract " +
                "(task_id, " +
                "session_id, " +
                "start_time, " +
                "search_keywords, " +
                "click_category_ids) VALUES (?,?,?,?,?);";
        Object[] params = new Object[]{sessionRandomExtract.getTaskid()
                ,sessionRandomExtract.getSessionid()
                ,sessionRandomExtract.getStartTime()
                ,sessionRandomExtract.getSearchKeywords()
                ,sessionRandomExtract.getClickCategoryIds()};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
