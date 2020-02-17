package mockData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import utils.DateUtils;
import utils.StringUtils;

import java.util.*;

public class MockData {

    private static final List<Row> rows = new ArrayList<>();
    private static final List<Row> rows2 = new ArrayList<>();
    private static final List<Row> rows3 = new ArrayList<>();

    private static Random random = new Random();

    private static void initDataList1(){
        String[] searchKeywords = {"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        String[] actions = {"search", "click", "order", "pay"};
        //很显然，我们只分析一天的用户
        String date = DateUtils.getTodayDate();
        //100个以内的用户
        for (int i = 0; i < 100; i++) {
            long userid = random.nextInt(100);
            //每个用户10个session，可能有重复用户的几十个session
            for (int j = 0; j < 10; j++) {
                //一个session对应一个小时
                String sessionid = UUID.randomUUID().toString().replace("-", "");
                String hourTime = date + " " + StringUtils.fullfill(String.valueOf(random.nextInt(23)));
                //一个session对应一个品类
                Long clickCategoryId = null;
                //一个session有不确定不到100的行为
                for (int k = 0; k < random.nextInt(100); k++) {
                    //随机访问0 ~ 9 的页面
                    long pageid = random.nextInt(10);
                    //访问时间 精确到 秒
                    String actionTime = hourTime + ":"
                            + StringUtils.fullfill(String.valueOf(random.nextInt(59)))
                            + ":" + StringUtils.fullfill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;
                    //访问行为
                    String action = actions[random.nextInt(4)];
                    switch (action){
                        case "search":
                            searchKeyword = searchKeywords[random.nextInt(10)];
                            break;
                        case "click":
                            if (clickCategoryId == null){
                                clickCategoryId = Long.parseLong(String.valueOf(random.nextInt(100)));
                            }
                            clickProductId = Long.parseLong(String.valueOf(random.nextInt(100)));
                            break;
                        case "order":
                            orderCategoryIds = String.valueOf(random.nextInt(100));
                            orderProductIds = String.valueOf(random.nextInt(100));
                            break;
                        default:
                            payCategoryIds = String.valueOf(random.nextInt(100));
                            payProductIds = String.valueOf(random.nextInt(100));
                    }
                    //创建一个row对象
                    Row row = RowFactory.create(date, userid, sessionid,
                            pageid, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds,
                            Long.valueOf(String.valueOf(random.nextInt(10))));

                    rows.add(row);
                }
            }
        }

    }

    private static void initDataList2(){

        String[] sexes = {"male", "female"};

        for (int i = 0; i < 100; i++) {
            long userid = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userid, username, name, age,
                    professional, city, sex);
            rows2.add(row);
        }
    }

    private static void initDataList3(){

        int[] productStatus = {0, 1};

        for (int i = 0; i < 100; i++) {
            long productId = i;
            String productName = "product" + i;
            String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";

            Row row = RowFactory.create(productId, productName, extendInfo);
            rows3.add(row);
        }
    }

    public static void mockData1(JavaSparkContext jsc, SQLContext sqlContext){

        initDataList1();

        JavaRDD<Row> rowsRdd = jsc.parallelize(rows);
        //动态创建schema
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("city_id", DataTypes.LongType, true)));

        Dataset<Row> df = sqlContext.createDataFrame(rowsRdd, schema);

        df.createOrReplaceTempView("user_visit_action");

    }

    public static void mockData2(JavaSparkContext jsc, SQLContext sqlContext){
        initDataList2();

        JavaRDD<Row> rowsRDD = jsc.parallelize(rows2);
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)));


        Dataset<Row> df = sqlContext.createDataFrame(rowsRDD, schema);

        df.createOrReplaceTempView("user_info");

    }

    public static void mockData3(JavaSparkContext jsc, SQLContext sqlContext){
        initDataList3();

        JavaRDD<Row> rowsRDD = jsc.parallelize(rows3);


        StructType schema3 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.LongType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)));

        Dataset<Row> df = sqlContext.createDataFrame(rowsRDD, schema3);

        df.createOrReplaceTempView("product_info");
    }
}
