package com.ibeifeng.sparkproject.test;

import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;

/**
 * Created by zkpk on 11/5/17.
 */
public class TaskDAOTest {
    public static void main(String[] args){
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(2);
        System.out.println(task.getTaskName());
    }
}
