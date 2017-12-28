package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.impl.*;

/**
 * Created by zkpk on 11/5/17.
 */
public class DAOFactory {
    
    public static ITaskDAO getTaskDAO(){
        return new TaskImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
        return new SessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO(){
        return new SessionDetailDAOImpl();
    }
    public static ITop10CategoryDAO getTop10CategoryDAO(){
        return new Top10CategoryDAOImpl();
    }
}
