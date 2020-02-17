package spark.session;

import com.alibaba.fastjson.JSONObject;
import constant.Constants;
import dao.ISessionDetailDAO;
import dao.ISessionRandomExtractDAO;
import dao.factory.DAOFactory;
import domain.SessionDetail;
import domain.SessionRandomExtract;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;
import utils.DateUtils;
import utils.ParamUtils;
import utils.StringUtils;
import utils.ValidUtils;

import java.util.*;

public class UserVisitSessionAnalyzeSpark {

    /**
     * 通过taskParam 获取指定范围内用户访问行为数据
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam){

        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action "
                + "where date>='" + startDate + "' " + "and date<='" + endDate + "'";

        Dataset<Row> rowsDS = sqlContext.sql(sql);

        return rowsDS.javaRDD();
    }

    /**
     * map -> 抽取sessionId为key
     */
    public static JavaPairRDD<String, Row> getSessionId2ActionRDD1(JavaRDD<Row> actionRDD){
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            private static final long serialVersionUID = 5180959517685737605L;

            @Override
            public Tuple2<String, Row> call(Row row){
                return new Tuple2<>(row.getString(2), row);
            }
        });
    }

    /**
     * mapPartitionsToPair -> 抽取sessionId为key
     * 可能会造成单点内存溢出
     */
    public static JavaPairRDD<String, Row> getSessionId2ActionRDD2(JavaRDD<Row> actionRDD){
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            private static final long serialVersionUID = 3792977436116696935L;

            @Override
            public Iterator<Tuple2<String, Row>> call(Iterator<Row> iterator){
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
     * session粒度聚合 ： 将session聚合后分析的数据与user_info表关联返回
     */
    public static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaPairRDD<String, Row> sessionId2actionRDD){
        //session粒度分组，目的是下一步将同一个session的数据进行分析计算
        JavaPairRDD<String, Iterable<Row>> session2actionsRDD = sessionId2actionRDD.groupByKey();
        //显然处理聚合后数据的逻辑，可以写在map算子中
        //userId2PartAggregateInfoRDD可得：用户一次session会话下，搜索关键词，点击品类别，访问时长，访问步长，session开始时间
        JavaPairRDD<Long, String> userId2PartAggregateInfoRDD = session2actionsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            private static final long serialVersionUID = -6476390418020587358L;

            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple){

                String sessionId = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();
                //session属于user
                Long userid = null;
                //求得该session所有的搜索词，并排除重复
                StringBuilder searchKeywordsBuffer = new StringBuilder();
                StringBuilder clickCategoryIdsBuffer = new StringBuilder();
                //通过比对session的最大时间和最小时间，计算session的持续时间
                Date startTime = null;
                Date endTime = null;
                //session访问步长
                int stepLength = 0;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    System.out.println(row);
                    if (userid == null) {
                        userid = row.getLong(1);
                    }

                    String searchKeyWord = row.getString(5);
                    Long clickCategoryId = null;
                    //搜索词和品类点击都有可能为null
                    if (StringUtils.isNotEmpty(row.getString(5))) {
                        if (!searchKeywordsBuffer.toString().contains(searchKeyWord)) {
                            searchKeywordsBuffer.append(searchKeyWord).append(",");
                        }
                    }
                    if (!row.isNullAt(6)) {
                        clickCategoryId = row.getLong(6);
                        if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                            clickCategoryIdsBuffer.append(clickCategoryId).append(",");
                        }
                    }
                    //获取该行为的发生时间
                    String string = row.getString(4);
                    System.out.println(string);
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
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
                //方便下一步用户聚合分析，将userId作为key
                return new Tuple2<>(userid, partAggrInfo);
            }
        });

        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        //map 提取 userId 方便join
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            private static final long serialVersionUID = 1777706946804972002L;

            @Override
            public Tuple2<Long, Row> call(Row row){
                return new Tuple2<>(row.getLong(0), row);
            }
        });
        //joined by userId
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggregateInfoRDD.join(userId2InfoRDD);

        //使用mapToPair处理join后的数据, 将key转为sessionId
        return userId2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            private static final long serialVersionUID = 7335812056370083434L;

            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple){
                String partAggregateInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;
                //提取sessionId
                String sessionId = StringUtils.getFieldFromConcatString(partAggregateInfo, "\\|", Constants.FIELD_SESSION_ID);
                //提取用户信息
                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);
                //拼接
                String fullAggregateInfo = partAggregateInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<>(sessionId, fullAggregateInfo);
            }
        });
    }

    /**
     * 根据task设定条件过滤筛选数据，同时对范围内数据进行访问时长和步长的统计
     */
    public static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,final JSONObject taskParam,final AccumulatorV2<String, String> sessionAggrStatAccumulator){
        //将JSON参数解析出来处理成字符串，为了后面性能优化做铺垫
        //年龄范围，职业，城市，性别，搜索关键词，品类id
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
        //拼接所有字段，如果为null默认空串
        String _params = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_params.endsWith("\\|")) {
            _params = _params.substring(0, _params.length() - 1);
        }
        //这是一种习惯，_parameter代表未处理完的变量
        final String params = _params;

        return sessionid2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            private static final long serialVersionUID = 5767496311946594260L;

            @Override
            public Boolean call(Tuple2<String, String> tuple) {
                String aggrInfo = tuple._2;
                //年龄范围
                if (!ValidUtils.between(aggrInfo,
                        Constants.FIELD_AGE, params, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) return false;
                //职业范围
                if (!ValidUtils.in(aggrInfo,
                        Constants.FIELD_PROFESSIONAL, params, Constants.PARAM_PROFESSIONALS)) return false;
                //城市范围
                if (!ValidUtils.in(aggrInfo,
                        Constants.FIELD_CITY, params, Constants.PARAM_CITIES)) return false;
                //性别
                if (!ValidUtils.equal(aggrInfo,
                        Constants.FIELD_SEX, params, Constants.PARAM_SEX)) return false;
                //搜索关键字匹配，只要包含一个即可
                if (!ValidUtils.in(aggrInfo,
                        Constants.FIELD_SEARCH_KEYWORDS, params, Constants.PARAM_KEYWORDS)) return false;
                //点击品类id
                if (!ValidUtils.in(aggrInfo,
                        Constants.FIELD_CLICK_CATEGORY_IDS, params, Constants.PARAM_CATEGORY_IDS)) return false;

                //对符合条件的数据加入统计访问步长和时长的计算
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                long visitLength = Long.parseLong(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.parseLong(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);

                return true;
            }
            //计算访问时长在什么区间，驱动累加器
            private void calculateVisitLength(long visitLength) {
                if(visitLength >=1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if(visitLength >=4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if(visitLength >=7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if(visitLength >=10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if(visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if(visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if(visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if(visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if(visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }
            //计算访问步长在什么区间，驱动累加器
            private void calculateStepLength(long stepLength) {
                if(stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if(stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if(stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if(stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if(stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if(stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });
    }

    /**
     *  关联明细数据
     */
    public static JavaPairRDD<String, Row> getSessionId2detailRDD(JavaPairRDD<String, String> sessionId2aggregateInfoRDD, JavaPairRDD<String, Row> sessionId2actionRDD){
        return sessionId2aggregateInfoRDD.join(sessionId2actionRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            private static final long serialVersionUID = 2046447115923588705L;

            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                return new Tuple2<>(tuple._1, tuple._2._2);
            }
        });
    }

    /**
     * 随机抽取
     */
    public static void randomExtractSession(JavaSparkContext jsc, final long taskId,
                                            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                            JavaPairRDD<String, Row> sessionid2actionRDD){
        /*
            计算每天每小时的session数量, 放入HashMap<yyyy-MM-dd, <HH, count>>
         */

        //分离出dateHour
        JavaPairRDD<String, String> dateHour2aggregateInfoRDD = sessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            private static final long serialVersionUID = -1505255304768570414L;
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                String aggregateInfo = tuple._2;
                String startTime = StringUtils.getFieldFromConcatString(aggregateInfo, "\\|", Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(startTime);
                return new Tuple2<>(dateHour, aggregateInfo);
            }
        });
        //统计每个小时session的数量
        Map<String, Long> countMap = dateHour2aggregateInfoRDD.countByKey();
        //初始化样本空间<yyyy-MM-dd, <HH, count>>
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();

        for (Map.Entry<String, Long> dateHourCount : countMap.entrySet()) {
            String dateHour = dateHourCount.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long hourCount = Long.parseLong(String.valueOf(dateHourCount.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.computeIfAbsent(date, k -> new HashMap<>());
            hourCountMap.put(hour, hourCount);
        }

        /*
            调用随机抽取算法，返回抽取索引列表。这里默认提取100个sample。
         */
        Map<String, Map<String, List<Integer>>> randomExtractMap = getRandomExtractMap(100, dateHourCountMap);
        //fastUtils优化
        Map<String, Map<String, IntList>> fastRandomExtractMap = getFastUtilMap(randomExtractMap);
        //广播变量
        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = jsc.broadcast(fastRandomExtractMap);
        /*
            分组后，根据Index列表抽取session数据写入mysql，同时返回extractSessionIdsRDD
         */
        JavaPairRDD<String, Iterable<String>> dateHour2sessionsRDD = dateHour2aggregateInfoRDD.groupByKey();
        JavaPairRDD<String, String> extractSessionIdsRDD = dateHour2sessionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            private static final long serialVersionUID = -3095819193387622421L;

            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();

                String dateHour = tuple._1;
                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];
                //一个hour内，多个session的信息
                Iterator<String> iterator = tuple._2.iterator();
                Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast.value();
                List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
                //将对应抽取的session数据写入mysql
                ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();
                int index = 0;
                while (iterator.hasNext()) {
                    String sessionAggregateInfo = iterator.next();
                    if (extractIndexList.contains(index)) {
                        String sessionId = StringUtils.getFieldFromConcatString(sessionAggregateInfo, "\\|", Constants.FIELD_SESSION_ID);
                        String startTime = StringUtils.getFieldFromConcatString(sessionAggregateInfo, "\\|", Constants.FIELD_START_TIME);
                        String searchKeyword = StringUtils.getFieldFromConcatString(sessionAggregateInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS);
                        String clickCategory = StringUtils.getFieldFromConcatString(sessionAggregateInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS);
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        sessionRandomExtract.setTaskId(taskId);
                        sessionRandomExtract.setSessionId(sessionId);
                        sessionRandomExtract.setStartTime(startTime);
                        sessionRandomExtract.setSearchKeywords(searchKeyword);
                        sessionRandomExtract.setClickCategoryIds(clickCategory);
                        sessionRandomExtractDAO.insert(sessionRandomExtract);

                        extractSessionIds.add(new Tuple2<>(sessionId, sessionId));
                    }
                    index++;
                }
                return extractSessionIds.iterator();
            }
        });

        /*
            获取抽取session的明细数据
         */

        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionIdsRDD.join(sessionid2actionRDD);
        
        //调用foreachPartition插入mysql
        extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
            private static final long serialVersionUID = 2195946490529058948L;

            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
                //将迭代出的一个分区内的session明细数据，统一写入mysql
                List<SessionDetail> sessionDetails = new ArrayList<>();

                while (iterator.hasNext()){
                    Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
                    Row row = tuple._2._2;

                    SessionDetail sessionDetail = new SessionDetail();
                    sessionDetail.setTaskId(taskId);
                    sessionDetail.setUserId(row.getLong(1));
                    sessionDetail.setSessionId(row.getString(2));
                    sessionDetail.setPageId(row.getLong(3));
                    sessionDetail.setActionTime(row.getString(4));
                    sessionDetail.setSearchKeyword(row.getString(5));
                    sessionDetail.setClickCategoryId(row.getLong(6));
                    sessionDetail.setClickProductId(row.getLong(7));
                    sessionDetail.setOrderCategoryIds(row.getString(8));
                    sessionDetail.setOrderProductIds(row.getString(9));
                    sessionDetail.setPayCategoryIds(row.getString(10));
                    sessionDetail.setPayProductIds(row.getString(11));

                    sessionDetails.add(sessionDetail);
                }

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insertBatch(sessionDetails);
            }
        });
    }

    /**
     * 随机抽取算法
     * sampleSpace代表一个样本空间（day）， group代表样本空间的一个分组（hour）， sample代表一个样本
     */
    private static Map<String, Map<String, List<Integer>>> getRandomExtractMap(int extractSampleNums, Map<String, Map<String, Long>> sampleSpacesMap){
        Random random = new Random();
        //总抽取数量确定，多个公平样本平均分配抽取数量
        int extractNumberPerSampleSpace = extractSampleNums / sampleSpacesMap.size();
        //初始化抽取样本分布HashMap
        HashMap<String, Map<String, List<Integer>>> extractMap = new HashMap<>();
        //遍历所有样本空间，计算抽取样本分布, 插入extractMap
        for(Map.Entry<String, Map<String, Long>> sampleSpace : sampleSpacesMap.entrySet()){
            String sampleSpaceKey = sampleSpace.getKey();
            Map<String, Long> groups = sampleSpace.getValue();
            //统计一个样本空间的样本数量
            long sampleSpaceCount = 0L;
            for(Long groupCount : groups.values()){
                sampleSpaceCount += groupCount;
            }
            /*
                等同于：
                Map<String, List<Integer>> extractSampleSpace = extractMap.get(sampleSpaceKey);
                if (extractSampleSpace == null){
                    extractSampleSpace = new HashMap<>();
                    extractMap.put(sampleSpaceKey, extractSampleSpace);
                }
             */
            Map<String, List<Integer>> extractSampleSpace = extractMap.computeIfAbsent(sampleSpaceKey, v -> new HashMap<>());

            for(Map.Entry<String, Long> group : groups.entrySet()){
                String groupKey = group.getKey();
                long groupCount = group.getValue();
                //同过本组sample数样本空间占比，计算应抽取sample数, int向下取整，不会超过总抽样，但可能返回总样本少于总抽样
                int extractNumsPerGroup =
                        (int)(((double) groupCount / (double) sampleSpaceCount) * extractNumberPerSampleSpace);
                //如果欲抽取样本过大，则抽取所有样本
                if(extractNumsPerGroup > groupCount){
                    extractNumsPerGroup = (int)groupCount;
                }
                /*
                    等同于：
                    List<Integer> extractSampleListPerGroup = extractSampleSpace.get(groupKey);
                if (extractSampleListPerGroup == null){
                    extractSampleListPerGroup = new ArrayList<>();
                    extractSampleSpace.put(groupKey, extractSampleListPerGroup);
                }
                 */
                List<Integer> extractSampleListPerGroup = extractSampleSpace.computeIfAbsent(groupKey, v -> new ArrayList<>());
                //group内随机抽取extractNumsPerGroup个sample
                for (int i = 0; i < extractNumsPerGroup; i++) {
                    int extractIndex = random.nextInt((int) groupCount);
                    while (extractSampleListPerGroup.contains(extractIndex)){
                        extractIndex =  random.nextInt((int) groupCount);
                    }
                    extractSampleListPerGroup.add(extractIndex);
                }
            }
        }

        return extractMap;
    }

    /**
     * 使用java扩展包fastUtils优化extractMap，减小广播变量大小
     */
    private static Map<String, Map<String, IntList>> getFastUtilMap(Map<String, Map<String, List<Integer>>> randomExtractMap){

        Map<String, Map<String, IntList>> fastRandomExtractMap = new HashMap<String, Map<String, IntList>>();
        for (Map.Entry<String, Map<String, List<Integer>>> sampleSpace : randomExtractMap.entrySet()){
            String sampleSpaceKey = sampleSpace.getKey();
            Map<String, List<Integer>> groups = sampleSpace.getValue();

            HashMap<String, IntList> fastMap = new HashMap<>();
            for (Map.Entry<String, List<Integer>> group : groups.entrySet()){
                String groupKey = group.getKey();
                List<Integer> sampleList = group.getValue();
                IntArrayList fastList = new IntArrayList();
                fastList.addAll(sampleList);
                fastMap.put(groupKey, fastList);
            }
            fastRandomExtractMap.put(sampleSpaceKey, fastMap);
        }
        return fastRandomExtractMap;
    }


}
