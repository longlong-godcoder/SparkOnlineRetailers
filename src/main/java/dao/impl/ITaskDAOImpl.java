package dao.impl;

import dao.ITaskDAO;
import domain.Task;
import jdbc.JDBCHelper;

import java.sql.ResultSet;

public class ITaskDAOImpl implements ITaskDAO {
    @Override
    public Task findById(Long taskid) {
        String sql = "select * from task where task_id=?";
        Object[] params = {taskid};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        Task task = new Task();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()){
                    task.setTaskid(rs.getLong(1));
                    task.setTaskName(rs.getString(2));
                    task.setCreateTime(rs.getString(3));
                    task.setStartTime(rs.getString(4));
                    task.setFinishTime(rs.getString(5));
                    task.setTaskType(rs.getString(6));
                    task.setTaskStatus(rs.getString(7));
                    task.setTaskParam(rs.getString(8));
                }
            }
        });

        return task;
    }
}
