package com.cauwt.sparkproject.dao.impl;

import com.cauwt.sparkproject.dao.IAdStatDAO;
import com.cauwt.sparkproject.domain.AdStat;
import com.cauwt.sparkproject.jdbc.JDBCHelper;
import com.cauwt.sparkproject.model.AdStatQueryResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zkpk on 2/5/18.
 */
public class AdStatDAOImpl implements IAdStatDAO {
    @Override
    public void updateBatch(List<AdStat> adStats) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        List<AdStat> insertAdStats = new ArrayList<>();
        List<AdStat> updateAdStats = new ArrayList<>();

        String selectSQL = "select count(*) from ad_stat " +
                "where `date` = ? and province = ? and city = ? and ad_id = ?";

        for(AdStat adStat : adStats){
            Object[] params = new Object[]{
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdId()
            };
            final AdStatQueryResult adStatQueryResult = new AdStatQueryResult();
            jdbcHelper.executeQuery(selectSQL, params,rs ->{
                if (rs.next()){
                    int count = rs.getInt(1);
                    adStatQueryResult.setCount(count);
                }
            });
            int count = adStatQueryResult.getCount();
            if (count >0){
                updateAdStats.add(adStat);
            } else {
                insertAdStats.add(adStat);
            }
        }
        // insert
        String insertSQL = "insert into ad_stat(`date`, province, city, ad_id, click_count)" +
                "values (?,?,?,?,?) ";
        List<Object[]> insertParamsList = new ArrayList<>();
        for(AdStat adStat: insertAdStats){
            Object[] params = new Object[]{
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdId(),
                    adStat.getClickCount()
            };
            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSQL,insertParamsList);
        
        // update
        String updateSQL = "update ad_stat set click_count = ? " +
                "where `date` = ? and province = ? and city = ? and ad_id = ? and click_count=?";
        List<Object[]> updateParamsList = new ArrayList<>();
        for(AdStat adStat: updateAdStats){
            Object[] params = new Object[]{
                    adStat.getClickCount(),
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdId()
            };
            updateParamsList.add(params);
        }
        jdbcHelper.executeBatch(updateSQL,updateParamsList);
    }
}
