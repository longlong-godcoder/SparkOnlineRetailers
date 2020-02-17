package dao.impl;

import dao.ISessionDetailDAO;
import domain.SessionDetail;
import jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class SessionDetailDAOImpl implements ISessionDetailDAO {

    @Override
    public void insertBatch(List<SessionDetail> sessionDetails) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        ArrayList<Object[]> paramsList = new ArrayList<>();
        for (SessionDetail sessionDetail : sessionDetails){
            Object[] params = {
                    sessionDetail.getTaskId(),
                    sessionDetail.getUserId(),
                    sessionDetail.getSessionId(),
                    sessionDetail.getPageId(),
                    sessionDetail.getActionTime(),
                    sessionDetail.getSearchKeyword(),
                    sessionDetail.getClickCategoryId(),
                    sessionDetail.getClickProductId(),
                    sessionDetail.getOrderCategoryIds(),
                    sessionDetail.getOrderProductIds(),
                    sessionDetail.getPayCategoryIds(),
                    sessionDetail.getPayProductIds()};

            paramsList.add(params);
        }
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }
}
