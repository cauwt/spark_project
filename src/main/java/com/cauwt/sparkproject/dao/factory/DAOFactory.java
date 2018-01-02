package com.cauwt.sparkproject.dao.factory;

import com.cauwt.sparkproject.dao.*;
import com.cauwt.sparkproject.dao.impl.*;

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
    public static ITop10CategorySessionDAO getTop10CategorySessionDAO(){
        return new Top10CategorySessionDAOImpl();
    }
}
