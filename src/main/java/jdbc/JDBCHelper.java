package jdbc;

import conf.ConfigurationManager;
import constant.Constants;

import java.sql.*;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class JDBCHelper {

    /*
        网上有说法，目前jdk新版本其实不需要加载driver。
        其实加载driver的原理无非就是执行一些类中的静态代码块，翻一下源代码就可以了
     */

    static {
        String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("加载JDBC Driver失败");
        }
    }
    //明显需要做成单例
    private static JDBCHelper instance = null;
    //另外也可以用静态内部类单例模式
    public static JDBCHelper getInstance(){
        if (instance == null){
            synchronized (JDBCHelper.class){
                if (instance == null){
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }
    //连接池, java链表包含了很多队列的功能, 可以用Deque接口声明
    private Deque<Connection> datasource = new LinkedList<>();
    //私有构造
    private JDBCHelper() {
        //通过配置文件获取连接池数量
        Integer datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);

        for (int i = 0; i < datasourceSize; i++) {
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);

            try {
                Connection connection = DriverManager.getConnection(url, user, password);
                datasource.push(connection);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("创建jdbc连接失败");
            }
        }
    }

    public synchronized Connection getConnection(){
        //如果连接池为空，等待获取连接
        while (datasource.size() == 0){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return datasource.poll();
    }

    /**
     * 更新操作
     */
    public int executeUpdate(String sql, Object[] params){
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        conn = getConnection();

        try {
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);
            if (params != null && params.length > 0){
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rtn = pstmt.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("JDBCHelper -- sql处理异常");
        }finally {
            //用完连接放回连接池
            if (conn != null){
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 查询操作
     */
    public void executeQuery(String sql, Object[] params, QueryCallBack callBack){
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        conn = getConnection();

        try {
            pstmt = conn.prepareStatement(sql);
            if (params != null && params.length > 0){
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            callBack.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("DBCHelper -- sql处理异常");
        }finally {
            if (conn != null){
                datasource.push(conn);
            }
        }
    }

    public int[] executeBatch(String sql, List<Object[]> paramsList){
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        conn = getConnection();

        try {
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);
            if (paramsList != null && paramsList.size() > 0){
                for (Object[] params : paramsList) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setObject(i + 1, params[i]);
                    }
                    pstmt.addBatch();
                }
            }

            rtn = pstmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("DBCHelper -- sql处理异常");
        }finally {
            if (conn != null){
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 查询调用接口
     */
    public static interface QueryCallBack{
        void process(ResultSet rs) throws Exception;
    }

    public static void main(String[] args) {

    }
}
