package dao.factory;

import dao.ITaskDAO;
import dao.impl.ITaskDAOImpl;

public class DAOFactory {

    public static ITaskDAO getTaskDAO(){
        return new ITaskDAOImpl();
    }
}
