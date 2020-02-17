package dao.factory;

import dao.ISessionDetailDAO;
import dao.ISessionRandomExtractDAO;
import dao.ITaskDAO;
import dao.impl.SessionDetailDAOImpl;
import dao.impl.SessionRandomExtractDAOImpl;
import dao.impl.TaskDAOImpl;

public class DAOFactory {

    public static ITaskDAO getTaskDAO(){
        return new TaskDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
        return new SessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO(){
        return new SessionDetailDAOImpl();
    }
}
