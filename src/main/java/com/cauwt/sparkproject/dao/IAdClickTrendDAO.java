package com.cauwt.sparkproject.dao;

import com.cauwt.sparkproject.domain.AdClickTrend;

import java.util.List;

/**
 * Created by zkpk on 2/5/18.
 */
public interface IAdClickTrendDAO {
    void updateBatch(List<AdClickTrend> adClickTrends);
}
