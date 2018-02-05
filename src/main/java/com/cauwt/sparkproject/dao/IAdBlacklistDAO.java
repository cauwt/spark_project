package com.cauwt.sparkproject.dao;

import com.cauwt.sparkproject.domain.AdBlacklist;

import java.util.List;

/**
 * Created by zkpk on 2/4/18.
 */
public interface IAdBlacklistDAO {
    void insertBatch(List<AdBlacklist> adBlackLists);
    List<AdBlacklist> findAll();
}
