package com.cauwt.sparkproject.dao;

import com.cauwt.sparkproject.domain.SessionDetail;

import java.util.List;

/**
 * Created by zkpk on 11/14/17.
 */
public interface ISessionDetailDAO {
    void insert(SessionDetail sessionDetail);
    void insertBatch(List<SessionDetail> sessionDetails);
}
