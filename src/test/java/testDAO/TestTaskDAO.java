package testDAO;

import dao.ITaskDAO;
import dao.factory.DAOFactory;
import domain.Task;

public class TestTaskDAO {

    public static void test(){
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1L);
        System.out.println(task);
    }

    public static void main(String[] args) {
        test();
    }
}
