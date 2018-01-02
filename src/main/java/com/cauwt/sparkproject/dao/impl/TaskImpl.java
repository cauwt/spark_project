package com.cauwt.sparkproject.dao.impl;

import com.cauwt.sparkproject.dao.ITaskDAO;
import com.cauwt.sparkproject.domain.Task;
import com.cauwt.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * Created by zkpk on 11/5/17.
 */
public class TaskImpl implements ITaskDAO {
    @Override
    public Task findById(long taskId) {
        final Task task = new Task();
        String sql = "select  task_id,\n" +
                "        task_name,\n" +
                "        create_time,\n" +
                "        start_time,\n" +
                "        finish_time,\n" +
                "        task_type,\n" +
                "        task_status,\n" +
                "        task_param \n" +
                " from task where task_id = ?";
        Object[] params = new Object[] {taskId};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql,params, (ResultSet rs) -> {
            if(rs.next()){
                String taskName = rs.getString(2);
                String createTime = rs.getString(3);
                String startTime = rs.getString(4);
                String finishTime = rs.getString(5);
                String taskType = rs.getString(6);
                String taskStatus = rs.getString(7);
                String taskParam = rs.getString(8);

                task.setTaskId(taskId);
                task.setCreateTime(createTime);
                task.setStartTime(startTime);
                task.setFinishTime(finishTime);
                task.setTaskName(taskName);
                task.setTaskType(taskType);
                task.setTaskStatus(taskStatus);
                task.setTaskParam(taskParam);
            }
        });
        return task;
    }
}
