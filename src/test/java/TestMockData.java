import mockData.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class TestMockData {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[4]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        MockData.mockData1(javaSparkContext, sqlContext);
        MockData.mockData2(javaSparkContext, sqlContext);
        MockData.mockData3(javaSparkContext, sqlContext);
        Dataset<Row> ds1 = sqlContext.sql("select * from user_visit_action");
        ds1.show();
        Dataset<Row> ds2 = sqlContext.sql("select * from user_info");
        ds2.show();
        Dataset<Row> ds3 = sqlContext.sql("select * from product_info");
        ds3.show();
    }
}
