package dao;

import domain.Task;

public interface ITaskDAO {

    Task findById(Long taskId);
}
