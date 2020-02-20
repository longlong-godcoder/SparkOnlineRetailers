package testMockData;

import mockData.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class TestMockData {

    private static JavaSparkContext jsc;
    private static SQLContext sqlContext;
    static {
        SparkSession sparkSession = SparkSession.builder().appName("TestMockData").master("local[4]").getOrCreate();
        jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        sqlContext = sparkSession.sqlContext();
    }

    public static void testMockData(){
        MockData.mockData1(jsc, sqlContext);
        MockData.mockData2(jsc, sqlContext);
        MockData.mockData3(jsc, sqlContext);
        Dataset<Row> ds1 = sqlContext.sql("select * from user_visit_action");
        ds1.show();
        Dataset<Row> ds2 = sqlContext.sql("select * from user_info");
        ds2.show();
        Dataset<Row> ds3 = sqlContext.sql("select * from product_info");
        ds3.show();
    }

    public static JavaSparkContext getJavaSparkContext(){
        return jsc;
    }

    public static SQLContext getSqlContext(){
        return sqlContext;
    }

    public static void main(String[] args) {

       TestMockData.testMockData();
    }
}
