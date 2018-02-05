package com.cauwt.sparkproject.dao.impl;

import com.cauwt.sparkproject.dao.IAdUserClickCountDAO;
import com.cauwt.sparkproject.domain.AdUserClickCount;
import com.cauwt.sparkproject.jdbc.JDBCHelper;
import com.cauwt.sparkproject.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zkpk on 2/4/18.
 */
public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {
    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        List<AdUserClickCount> insertAdUserClickCountList = new ArrayList<>();
        List<AdUserClickCount> updateAdUserClickCountList = new ArrayList<>();
        // 1. check whether the record exists
        String selectSQL = "select count(*) from `ad_user_click_count` " +
                "where `date` = ? and `user_id` =? and `ad_id` = ?";
        for(AdUserClickCount adUserClickCount: adUserClickCounts){
            Object[] params = new Object[]{adUserClickCount.getDate(),
            adUserClickCount.getUserId(),
            adUserClickCount.getAdId()};
            final AdUserClickCountQueryResult adUserClickCountQueryResult =  new AdUserClickCountQueryResult();
            jdbcHelper.executeQuery(selectSQL, params, rs -> {
                while(rs.next()){
                    int count = rs.getInt(1);
                    adUserClickCountQueryResult.setCount(count);
                }
            });
            int count = adUserClickCountQueryResult.getCount();
            if (count >0){
                updateAdUserClickCountList.add(adUserClickCount);
            } else {
                insertAdUserClickCountList.add(adUserClickCount);
            }
        }
        
        // insert 
        String insertSQL = "insert into ad_user_click_count(`date`,user_id, ad_id, click_count) " +
                "values(?, ?, ?, ?);";
        List<Object[]> insertParamsList = new ArrayList<>();
        for(AdUserClickCount adUserClickCount: insertAdUserClickCountList){
            Object[] params = new Object[]{adUserClickCount.getDate(),
                    adUserClickCount.getUserId(),
                    adUserClickCount.getAdId(),
                    adUserClickCount.getClickCount()};
            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSQL, insertParamsList);
        
        // update
        String updateSQL = "update ad_user_click_count set click_count = click_count + ? " +
                "where `date` = ? and `user_id` =? and `ad_id` = ?;";
        List<Object[]> updateParamsList = new ArrayList<>();
        for(AdUserClickCount adUserClickCount: updateAdUserClickCountList){
            Object[] params = new Object[]{adUserClickCount.getClickCount(),
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserId(),
                    adUserClickCount.getAdId()};

            updateParamsList.add(params);
        }
        jdbcHelper.executeBatch(updateSQL, updateParamsList);

    }

    @Override
    public Long findClickCountByMultiKey(String date, Long userId, Long adId) {
        Long clickCount =0L;
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String selectSQL = "select click_count from ad_user_click_count " +
                "where `date`= ? and user_id = ? and ad_id= ?";
        Object[] params = new Object[]{
                date,
                userId,
                adId
        };
        AdUserClickCountQueryResult adUserClickCountQueryResult = new AdUserClickCountQueryResult();
        jdbcHelper.executeQuery(selectSQL, params, rs -> {
            while(rs.next()){
                adUserClickCountQueryResult.setClickCount(Long.valueOf(rs.getInt(1)));
            }
        });
        clickCount = adUserClickCountQueryResult.getClickCount();
        return clickCount;
    }
}
