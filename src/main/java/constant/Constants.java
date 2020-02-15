package constant;

/**
 * 常量接口
 */
public interface Constants {
    //jdbc配置参数
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";

    //Kafka配置参数
    String METADATA_BROKER_LIST = "metadata.broker.list";
    String SERIALIZER_CLASS = "serializer.class";
}
