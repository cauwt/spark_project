package com.cauwt.sparkproject.dao.impl;

import com.cauwt.sparkproject.dao.IAdClickTrendDAO;
import com.cauwt.sparkproject.domain.AdClickTrend;
import com.cauwt.sparkproject.jdbc.JDBCHelper;
import com.cauwt.sparkproject.model.AdClickTrendQueryResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zkpk on 2/5/18.
 */
public class AdClickTrendDAOImpl implements IAdClickTrendDAO {
    @Override
    public void updateBatch(List<AdClickTrend> adClickTrends) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        List<AdClickTrend> insertAdClickTrends = new ArrayList<>();
        List<AdClickTrend> updateAdClickTrends = new ArrayList<>();

        String selectSQL = "select count(*) from ad_click_trend " +
                "where `date` = ? and hour = ? and minute = ? and ad_id = ?";

        for(AdClickTrend adClickTrend : adClickTrends){
            Object[] params = new Object[]{
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdId()
            };
            final AdClickTrendQueryResult adClickTrendQueryResult = new AdClickTrendQueryResult();
            jdbcHelper.executeQuery(selectSQL, params,rs ->{
                if (rs.next()){
                    int count = rs.getInt(1);
                    adClickTrendQueryResult.setCount(count);
                }
            });
            int count = adClickTrendQueryResult.getCount();
            if (count >0){
                updateAdClickTrends.add(adClickTrend);
            } else {
                insertAdClickTrends.add(adClickTrend);
            }
        }
        // insert
        String insertSQL = "insert into ad_click_trend(`date`, hour, minute, ad_id, click_count)" +
                "values (?,?,?,?,?) ";
        List<Object[]> insertParamsList = new ArrayList<>();
        for(AdClickTrend adClickTrend: insertAdClickTrends){
            Object[] params = new Object[]{
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdId(),
                    adClickTrend.getClickCount()
            };
            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSQL,insertParamsList);
        
        // update
        String updateSQL = "update ad_click_trend set click_count = ? " +
                "where `date` = ? and hour = ? and minute = ? and ad_id = ?";
        List<Object[]> updateParamsList = new ArrayList<>();
        for(AdClickTrend adClickTrend: updateAdClickTrends){
            Object[] params = new Object[]{
                    adClickTrend.getClickCount(),
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdId()
            };
            updateParamsList.add(params);
        }
        jdbcHelper.executeBatch(updateSQL,updateParamsList);
    }
}
