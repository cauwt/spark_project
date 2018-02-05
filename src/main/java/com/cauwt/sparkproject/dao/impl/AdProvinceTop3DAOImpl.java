package com.cauwt.sparkproject.dao.impl;

import com.cauwt.sparkproject.dao.IAdProvinceTop3DAO;
import com.cauwt.sparkproject.domain.AdProvinceTop3;
import com.cauwt.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zkpk on 2/5/18.
 */
public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO {

    @Override
    public void updateBatch(List<AdProvinceTop3> adProvinceTop3s) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        // distinct date and province
        List<String> dateProvinces = new ArrayList<>();
        for(AdProvinceTop3 adProvinceTop3: adProvinceTop3s){
            String date = adProvinceTop3.getDate();
            String province = adProvinceTop3.getProvince();
            String dateProvince = date+"_"+province;
            if(!dateProvinces.contains(dateProvince)){
                dateProvinces.add(dateProvince);
            }
        }

            // delete the combinations of date and province that exist.
        String deleteSQL = "delete from ad_province_top3" +
                "where date = ? and province = ?";
        List<Object[]> deleteParamsList = new ArrayList<>();
        for(String dateProvince: dateProvinces){
            String[] dateProvinceSplitted = dateProvince.split("_");
            String date = dateProvinceSplitted[0];
            String province = dateProvinceSplitted[1];

            Object[] params = new Object[]{
                    date,
                    province
            };
            deleteParamsList.add(params);
        }
        jdbcHelper.executeBatch(deleteSQL,deleteParamsList);

        // insert new
        String insertSQL = "insert into ad_province_top3 (date, province, ad_id, click_count)" +
                "values (?, ?, ?, ?)";
        List<Object[]> insertParamsList = new ArrayList<>();
        for(AdProvinceTop3 adProvinceTop3: adProvinceTop3s){
            Object[] params = new Object[]{
                    adProvinceTop3.getDate(),
                    adProvinceTop3.getProvince(),
                    adProvinceTop3.getAdId(),
                    adProvinceTop3.getClickCount()
            };
            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSQL,insertParamsList);
        
        
    }
}
