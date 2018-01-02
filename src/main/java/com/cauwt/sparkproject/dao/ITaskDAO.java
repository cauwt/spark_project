package com.cauwt.sparkproject.dao;

import com.cauwt.sparkproject.domain.Task;

/**
 * Created by zkpk on 11/5/17.
 */
public interface ITaskDAO {

    /**
     *
     * @param taskId
     * @return
     */
    Task findById(long taskId);
}
