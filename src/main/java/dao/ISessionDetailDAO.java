package dao;

import domain.SessionDetail;

import java.util.List;

public interface ISessionDetailDAO {

//    /**
//     * 插入一条明细数据
//     */
//    void insert();

    /**
     * 批量插入明细数据
     */
    void insertBatch(List<SessionDetail> sessionDetails);
}
