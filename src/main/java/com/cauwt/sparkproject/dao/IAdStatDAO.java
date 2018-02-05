package com.cauwt.sparkproject.dao;

import com.cauwt.sparkproject.domain.AdStat;

import java.util.List;

/**
 * Created by zkpk on 2/5/18.
 */
public interface IAdStatDAO {
    void updateBatch(List<AdStat> adStats);
}
