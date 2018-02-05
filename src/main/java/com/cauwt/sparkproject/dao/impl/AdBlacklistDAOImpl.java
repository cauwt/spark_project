package com.cauwt.sparkproject.dao.impl;

import com.cauwt.sparkproject.dao.IAdBlacklistDAO;
import com.cauwt.sparkproject.domain.AdBlacklist;
import com.cauwt.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zkpk on 2/4/18.
 */
public class AdBlacklistDAOImpl implements IAdBlacklistDAO {
    @Override
    public void insertBatch(List<AdBlacklist> adBlackLists) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String insertSQL = "insert into ad_blacklist(user_id) values(?);";
        List<Object[]> paramsList = new ArrayList<>();
        for(AdBlacklist adBlacklist: adBlackLists){
            Object[] params = new Object[]{adBlacklist.getUserId()};
            paramsList.add(params);
        }
        jdbcHelper.executeBatch(insertSQL,paramsList);
    }

    @Override
    public List<AdBlacklist> findAll() {
        List<AdBlacklist> adBlacklists = new ArrayList<>();
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String selectSQL = "select user_id from ad_blacklist;";
        Object[] params = new Object[]{};
        jdbcHelper.executeQuery(selectSQL, params,rs ->{
            while(rs.next()){
                Long userId = rs.getLong(1);
                AdBlacklist adBlacklist = new AdBlacklist();
                adBlacklist.setUserId(userId);
            }
        });
        return adBlacklists;
    }
}
