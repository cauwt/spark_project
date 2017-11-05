package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

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
        JDBCHelper jdbcHelper = JDBCHelper.getInstaince();
        jdbcHelper.executeQuery(sql,params, (ResultSet rs) -> {
            if(rs.next()){
                long taskid = rs.getLong(1);
                String taskName = rs.getString(2);
                String createTime = rs.getString(3);
                String startTime = rs.getString(4);
                String finishTime = rs.getString(5);
                String taskType = rs.getString(6);
                String taskStatus = rs.getString(7);
                String taskParam = rs.getString(8);

                task.setTaskid(taskid);
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
