package dao.impl;

import dao.ISessionAggregateStatDAO;
import domain.SessionAggregateStat;
import jdbc.JDBCHelper;

public class SessionAggregateStatDAOImpl implements ISessionAggregateStatDAO {

    @Override
    public void insert(SessionAggregateStat sessionAggregateStat) {
        String sql = "insert into session_aggregate_stat "
                + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{sessionAggregateStat.getTaskid(),
                sessionAggregateStat.getSession_count(),
                sessionAggregateStat.getVisit_length_1s_3s_ratio(),
                sessionAggregateStat.getVisit_length_4s_6s_ratio(),
                sessionAggregateStat.getVisit_length_7s_9s_ratio(),
                sessionAggregateStat.getVisit_length_10s_30s_ratio(),
                sessionAggregateStat.getVisit_length_30s_60s_ratio(),
                sessionAggregateStat.getVisit_length_1m_3m_ratio(),
                sessionAggregateStat.getVisit_length_3m_10m_ratio(),
                sessionAggregateStat.getVisit_length_10m_30m_ratio(),
                sessionAggregateStat.getVisit_length_30m_ratio(),
                sessionAggregateStat.getStep_length_1_3_ratio(),
                sessionAggregateStat.getStep_length_4_6_ratio(),
                sessionAggregateStat.getStep_length_7_9_ratio(),
                sessionAggregateStat.getStep_length_10_30_ratio(),
                sessionAggregateStat.getStep_length_30_60_ratio(),
                sessionAggregateStat.getStep_length_60_ratio()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
