package com.cauwt.sparkproject.dao;

import com.cauwt.sparkproject.domain.AdProvinceTop3;

import java.util.List;

/**
 * Created by zkpk on 2/5/18.
 */
public interface IAdProvinceTop3DAO {
    void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
}
