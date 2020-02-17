package dao;

import domain.SessionRandomExtract;

/**
 * 随机抽取session DAO接口
 */
public interface ISessionRandomExtractDAO {
    /**
     * 插入随机抽取数据
     */
    void insert(SessionRandomExtract sessionRandomExtract);
}
