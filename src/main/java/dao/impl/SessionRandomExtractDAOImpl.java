package dao.impl;

import dao.ISessionRandomExtractDAO;
import domain.SessionRandomExtract;
import jdbc.JDBCHelper;

/**
 * 随机抽取session DAO实现类
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        //插入对应SessionRandomExtract属性的5个字段数据
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        Object[] params = {
                sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
