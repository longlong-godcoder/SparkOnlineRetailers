package mockData;

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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class MockData {

    private static final List<Row> rows1 = new ArrayList<>();
    private static final List<Row> rows2 = new ArrayList<>();
    private static final List<Row> rows3 = new ArrayList<>();

    private static Random random = new Random();

    private static void initDataList1(){
        String[] searchKeywords = {"床上用品", "家具", "零食", "饼干",
                "衣服", "书籍", "苹果手机", "笔记本电脑", "电视", "电子产品"};
        String[] actions = {"search", "click", "order", "pay"};
        //默认只产生一天的用户记录
        String date = DateUtils.getTodayDate();
        //100个以内的用户
        for (int i = 0; i < 100; i++) {
            long userId = random.nextInt(100);
            //每个用户10个session，可能有重复用户的几十个session
            for (int j = 0; j < 10; j++) {
                //一个session对应一个小时
                String sessionId = UUID.randomUUID().toString().replace("-", "");
                String hourTime = date + " " + StringUtils.fullfill(String.valueOf(random.nextInt(23)));
                //一个session对应一个品类
                Long clickCategoryId = null;
                //一个session有不确定不到100的行为
                for (int k = 0; k < random.nextInt(100); k++) {
                    //随机访问0 ~ 9 的页面
                    long pageId = random.nextInt(10);
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
                                clickCategoryId = Long.parseLong(String.valueOf(random.nextInt(50)));
                            }
                            clickProductId = Long.parseLong(String.valueOf(random.nextInt(50)));
                            break;
                        case "order":
                            orderCategoryIds = String.valueOf(random.nextInt(50));
                            orderProductIds = String.valueOf(random.nextInt(50));
                            break;
                        default:
                            payCategoryIds = String.valueOf(random.nextInt(50));
                            payProductIds = String.valueOf(random.nextInt(50));
                    }
                    //创建一个row对象
                    Row row = RowFactory.create(date, userId, sessionId,
                            pageId, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds,
                            Long.valueOf(String.valueOf(random.nextInt(10))));

                    rows1.add(row);
                }
            }
        }

    }

    private static void initDataList2(){

        String[] sexes = {"male", "female"};

        for (int i = 0; i < 50; i++) {
            long userId = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(50);
            String city = "city" + random.nextInt(50);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userId, username, name, age,
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
        printData(rows1,
                "data/user_visit_action.txt",
                "[1]date[2]user_id[3]session_id[4]page_id[5]action_time" +
                        "[6]search_keyword[7]click_category_id[8]click_product_id" +
                        "[9]order_category_ids[10]order_product_ids[11]pay_category_ids" +
                        "[12]pay_product_ids[13]city_id");
        JavaRDD<Row> rowsRdd = jsc.parallelize(rows1);
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
        printData(rows2,
                "data/user_info",
                "[1]user_id[2]username[3]name[4]age[5]professional[6]city[7]sex");
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
        printData(rows3,
                "data/product_info",
                "[1]product_id[2]product_name[3]extend_info");
        JavaRDD<Row> rowsRDD = jsc.parallelize(rows3);


        StructType schema3 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.LongType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)));

        Dataset<Row> df = sqlContext.createDataFrame(rowsRDD, schema3);

        df.createOrReplaceTempView("product_info");
    }

    public static void printData(List<Row> rows, String path, String header){
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path));
            bufferedWriter.write(header);
            bufferedWriter.newLine();

            for (Row row : rows) {
                bufferedWriter.write(row.toString());
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("================MockData.printData()  ERROR=====================");
        }
    }


}
