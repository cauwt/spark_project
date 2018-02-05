package com.cauwt.sparkproject.dao;

import com.cauwt.sparkproject.domain.AdUserClickCount;

import java.util.List;

/**
 * Created by zkpk on 2/4/18.
 */
public interface IAdUserClickCountDAO {
    void updateBatch(List<AdUserClickCount> adUserClickCountList);
    Long findClickCountByMultiKey(String date, Long userId, Long adId);
}
