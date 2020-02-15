package spark.session;

import com.alibaba.fastjson.JSONObject;
import constant.Constants;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import utils.DateUtils;
import utils.ParamUtils;
import utils.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

public class UserVisitSessionAnalyzeSpark {

    /**
     * 通过taskParam 获取指定范围内用户访问行为数据
     */
    public static JavaRDD<Row> getAcitionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam){

        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action "
                + "where date>='" + startDate + "' " + "and date<='" + endDate + "'";

        Dataset<Row> rowsDS = sqlContext.sql(sql);

        return rowsDS.javaRDD();
    }
    /**
     * map -> 抽取sessionid为key
     */
    public static JavaPairRDD<String, Row> getSessionid2ActionRDD1(JavaRDD<Row> actionRDD){
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
    }

    /**
     * mapPartitionsToPair -> 抽取sessionid为key
     * 可能会造成单点内存溢出
     */
    public static JavaPairRDD<String, Row> getSessionid2ActionRDD2(JavaRDD<Row> actionRDD){
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterator<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                ArrayList<Tuple2<String, Row>> list = new ArrayList<>();
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    list.add(new Tuple2<>(row.getString(2), row));
                }

                return list.iterator();
            }
        });
    }

    /**
     * session粒度聚合，可得：用户一次session会话下，搜索关键词，点击品类别，访问时长，访问步长，session开始时间
     * @return
     */
    public static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaPairRDD<String, Row> sessinoid2actionRDD){
        //session粒度分组，目的是下一步将同一个session的数据进行分析计算
        JavaPairRDD<String, Iterable<Row>> session2actionsRDD = sessinoid2actionRDD.groupByKey();
        //显然处理聚合后数据的逻辑，可以写在map算子中
        //userid2PartAggrInfoRDD可得：用户一次session会话下，搜索关键词，点击品类别，访问时长，访问步长，session开始时间
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = session2actionsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                String sessionid = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();
                //session属于user
                Long userid = null;
                //求得该session所有的搜索词，并排除重复
                StringBuilder searchKeywordsBuffer = new StringBuilder();
                StringBuilder clickCategoryIdsBuffer = new StringBuilder();
                //通过比对sesion的最大时间和最小时间，计算session的持续时间
                Date startTime = null;
                Date endTime = null;
                //session访问步长
                int stepLength = 0;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (userid == null) {
                        userid = row.getLong(1);
                    }

                    String searchKeyWord = row.getString(5);
                    Long clickCategoryId = row.getLong(6);
                    //搜索词和品类点击都有可能为null
                    if (StringUtils.isNotEmpty(searchKeyWord)) {
                        if (!searchKeywordsBuffer.toString().contains(searchKeyWord)) {
                            searchKeywordsBuffer.append(searchKeyWord + ",");
                        }
                    }
                    if (clickCategoryId != null) {
                        if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                            clickCategoryIdsBuffer.append(clickCategoryId + ",");
                        }
                    }
                    //获取该行为的发生时间
                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if (startTime == null) {
                        startTime = actionTime;
                    }
                    if (endTime == null) {
                        endTime = actionTime;
                    }
                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }

                    stepLength++;
                }
                //处理多余逗号
                String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                //计算session访问时长
                long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
                //计算结果拼接字符串
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
                //方便下一步用户聚合分析，将userid作为key
                return new Tuple2<>(userid, partAggrInfo);
            }
        });

        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(row.getLong(0)))
            }
        })

    }
}
