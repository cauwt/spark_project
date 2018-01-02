package com.cauwt.sparkproject.test;

import com.cauwt.sparkproject.dao.ITaskDAO;
import com.cauwt.sparkproject.dao.factory.DAOFactory;
import com.cauwt.sparkproject.domain.Task;

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
