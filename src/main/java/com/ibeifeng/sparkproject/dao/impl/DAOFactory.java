package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITaskDAO;

/**
 * Created by zkpk on 11/5/17.
 */
public class DAOFactory {
    
    public static ITaskDAO getTaskDAO(){
        return new TaskImpl();
    }
}
