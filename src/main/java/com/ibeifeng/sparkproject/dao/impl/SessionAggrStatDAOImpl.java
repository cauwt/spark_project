package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.domain.SessionAggrStat;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by zkpk on 11/14/17.
 */
public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO {
    @Override
    public void insert(SessionAggrStat sessionAggrStat) {
        String sql = "insert into session_aggr_stat(" +
                "`task_id`,`session_count`," +
                "`1s_3s`,`4s_6s`,`7s_9s`,`10s_30s`,`30s_60s`," +
                "`1m_3m`,`3m_10m`,`10m_30m`,`30m`," +
                "`1_3`,`4_6`,`7_9`,`10_30`,`30_60`,`60`) " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[]{sessionAggrStat.getTaskid()
                ,sessionAggrStat.getSession_count()
                ,sessionAggrStat.getTime_period_1s_3s_ratio()
                ,sessionAggrStat.getTime_period_4s_6s_ratio()
                ,sessionAggrStat.getTime_period_7s_9s_ratio()
                ,sessionAggrStat.getTime_period_10s_30s_ratio()
                ,sessionAggrStat.getTime_period_30s_60s_ratio()
                ,sessionAggrStat.getTime_period_1m_3m_ratio()
                ,sessionAggrStat.getTime_period_3m_10m_ratio()
                ,sessionAggrStat.getTime_period_10m_30m_ratio()
                ,sessionAggrStat.getTime_period_30m_ratio()
                ,sessionAggrStat.getStep_period_1_3_ratio()
                ,sessionAggrStat.getStep_period_4_6_ratio()
                ,sessionAggrStat.getStep_period_7_9_ratio()
                ,sessionAggrStat.getStep_period_10_30_ratio()
                ,sessionAggrStat.getStep_period_30_60_ratio()
                ,sessionAggrStat.getStep_period_60_ratio()};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
