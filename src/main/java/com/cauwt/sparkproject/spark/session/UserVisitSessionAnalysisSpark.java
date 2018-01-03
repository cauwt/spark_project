package com.cauwt.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.cauwt.sparkproject.conf.ConfigurationManager;
import com.cauwt.sparkproject.constant.Constants;
import com.cauwt.sparkproject.dao.*;
import com.cauwt.sparkproject.dao.factory.DAOFactory;
import com.cauwt.sparkproject.domain.*;
import com.cauwt.sparkproject.spark.MockData;
import com.cauwt.sparkproject.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import java.util.*;
import org.apache.spark.api.java.Optional;


/**
 * Created by zkpk on 11/5/17.
 */
public class UserVisitSessionAnalysisSpark {
    public static void main(String[] args) {
        // 1. create spark session
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName(Constants.SPARK_APP_NAME_SESSION)
                .getOrCreate();
        spark.conf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        spark.conf().set("spark.memory.storageFraction","0.5");
        spark.conf().set("spark.shuffle.consolidateFiles","true");
        spark.conf().set("spark.kryo.registrator", ToKryoRegistrator.class.getName());
        mockData(spark);

        long taskId = ParamUtils.getTaskIdFromArgs(args);


        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        //get action rdd
        JavaRDD<Row> actionRDD = getActionRDD(spark,taskParam);

        // get sessionId 2 action and persist it
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);
        sessionId2ActionRDD = sessionId2ActionRDD.persist(StorageLevel.MEMORY_ONLY());

        // get sessionId to AggrInfo : <sessionId,<sessionId,searchKeywords,clickCategoryIds,age,professional,city,sex>>
        JavaPairRDD<String,String> sessionId2AggrInfoRDD =
                aggregateBySession(spark,sessionId2ActionRDD);

        //refactor, filter and do stats
        SessionAggrStatAccumulator sessionAggrAccumulator = new SessionAggrStatAccumulator();
        spark.sparkContext().register(sessionAggrAccumulator);
        //<sessionId,<sessionId,searchKeywords,clickCategoryIds,age,professional,city,sex>>
        JavaPairRDD<String,String> filteredSessionId2AggrInfoRDD =
                filterSessionAndAggrStat(sessionId2AggrInfoRDD,taskParam,sessionAggrAccumulator);
        filteredSessionId2AggrInfoRDD = filteredSessionId2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

        // get shared sessionId2DetailRDD for later usages
        JavaPairRDD<String, Row> sessionId2DetailRDD = getSessionId2DetailRDD(
                filteredSessionId2AggrInfoRDD,sessionId2ActionRDD);
        sessionId2DetailRDD = sessionId2DetailRDD.persist(StorageLevel.MEMORY_ONLY());



        // trigger spark job
        System.out.println(filteredSessionId2AggrInfoRDD.count());
        // calculate ratios for session groups defined by visit length and step length
        calculateAndPersistAggrStat(sessionAggrAccumulator.value(), task.getTaskId());

        // extract sessions randomly
        randomExtractSession(spark,task.getTaskId()
                , filteredSessionId2AggrInfoRDD
                , sessionId2DetailRDD);


        // feature 3: get top 10 categories most clicked, ordered and paid.
        List<Tuple2<CategorySortKey,String>> top10CategoryList = getTop10Category(taskId, sessionId2DetailRDD);

        // feature 4: get top 10 active sessions
        getTop10CategorySession(spark,taskId, top10CategoryList, sessionId2DetailRDD);

        spark.stop();
    }



    /**
     * mock data : user, product, and visit action
     * @param spark
     */
    private static void mockData(SparkSession spark) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            MockData.mock(spark);
        }
    }

    /**
     * deprecated
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * get actions filtered by date
     * @param spark
     * @param taskParam
     * @return action info: date,user_id,sessionId,...
     */
    private static JavaRDD<Row> getActionRDD(SparkSession spark, JSONObject taskParam){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String sql = "select  " +
                "`date`" +
                ", user_id" +
                ", session_id" +
                ", page_id" +
                ", action_time" +
                ", search_keyword" +
                ", click_category_id" +
                ", click_product_id" +
                ", order_category_ids" +
                ", order_product_ids" +
                ", pay_category_ids" +
                ", pay_product_ids" +
                ", city_id " +
                "from user_visit_action where date >='"+startDate+"' and date <= '"+endDate+"'";
        Dataset<Row> actionDF = spark.sql(sql);
        actionDF.cache();
        return actionDF.javaRDD();
    }

    /**
     * get map of sessionId to action details
     * @param actionRDD
     * @return sessionId, row(session details)
     */
    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        //return actionRDD.mapToPair(row -> new Tuple2<>(row.getString(2),row));
        return actionRDD.mapPartitionsToPair(rows -> {
            List<Tuple2<String,Row>> list = new ArrayList<>();
            while (rows.hasNext()){
                Row row = rows.next();
                list.add(new Tuple2<>(row.getString(2),row));
            }
            return list.iterator();
        });
    }

    /**
     * do sessiton aggregration
     * @param spark
     * @param sessionId2ActionRDD
     * @return <sessionId,<sessionId,searchKeywords,clickCategoryIds,age,professional,city,sex>>
     */
    private static JavaPairRDD<String,String> aggregateBySession(SparkSession spark, JavaPairRDD<String,Row> sessionId2ActionRDD){
        //map to <sessionId, Row>
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD =  sessionId2ActionRDD.groupByKey();

        //<userId, partAggrInfo(sessionId, searchKeywords, clickCategoryIds)>
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD =  sessionId2ActionsRDD.mapToPair(
                (PairFunction<Tuple2<String,Iterable<Row>>, Long, String> ) tuple -> {
                    String sessionId = tuple._1;
                    Iterator<Row> iterator = tuple._2.iterator();
                    StringBuffer searchKeywordsBuffer = new StringBuffer("");
                    StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                    Long userId = null;
                    Date startTime = null;
                    Date endTime = null;
                    int stepLength = 0;

                    while(iterator.hasNext()){
                        Row row = iterator.next();
                        String searchKeyword = row.getString(5);

                        Long clickCategoryId = row.isNullAt(6)?null:row.getLong(6);
                        if(userId == null){
                            userId = row.getLong(1);
                        }


                        if(StringUtils.isNotEmpty(searchKeyword)) {
                            if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                searchKeywordsBuffer.append(searchKeyword + ",");
                            }
                        }
                        if(clickCategoryId != null) {
                            if(!clickCategoryIdsBuffer.toString().contains(
                                    String.valueOf(clickCategoryId))) {
                                clickCategoryIdsBuffer.append(clickCategoryId + ",");
                            }
                        }

                        // get the startTime and endTime of a session
                        Date actionTime = DateUtils.parseTime(row.getString(4));
                        if(startTime == null){
                            startTime = actionTime;
                        }
                        if(endTime == null){
                            endTime = actionTime;
                        }
                        if(actionTime.before(startTime)){
                            startTime = actionTime;
                        }
                        if(actionTime.after(endTime)){
                            endTime = actionTime;
                        }

                        // get step length
                        stepLength ++;


                    }
                    // calculate visit_length (in second)
                    Long visitLength = (endTime.getTime() - startTime.getTime())/1000;

                    String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                    String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                    // return <userId, partAggrInfo>

                    String partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionId
                            + "|"+Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeywords
                            + "|"+Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds
                            + "|"+Constants.FIELD_VISIT_LENGTH +"="+visitLength
                            + "|"+Constants.FIELD_STEP_LENGTH +"="+stepLength
                            + "|"+Constants.FIELD_START_TIME +"="+DateUtils.formatTime(startTime);
                    return new Tuple2<>(userId,partAggrInfo);
                });


        String sql = "select user_id,username,name,age,professional,city,sex from user_info";
        JavaRDD<Row> userInfoRDD = spark.sql(sql).toJavaRDD();
        JavaPairRDD<Long,Row> userId2InfoRDD = userInfoRDD.mapToPair((PairFunction<Row,Long,Row>) (Row row) ->
                new Tuple2<>(row.getLong(0),row)) ;

        //join sessionAggr and user
        // return <userId, <partAggrInfo, userFullInfo>>
        JavaPairRDD<Long,Tuple2<String,Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);

        //return <sessionId,fullAggrInfo(age,professional,city,sex)>
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(
                (PairFunction<Tuple2<Long,Tuple2<String,Row>>,String,String>) (tuple) ->{
                    String partAggrInfo = tuple._2._1;
                    Row row = tuple._2._2;
                    String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                    int age = row.getInt(3);
                    String professional = row.getString(4);
                    String city = row.getString(5);
                    String sex = row.getString(6);

                    String fullAggrInf = partAggrInfo +"|"
                            +Constants.FIELD_AGE+"="+age+"|"
                            +Constants.FIELD_PROFESSIONAL+"="+professional+"|"
                            +Constants.FIELD_CITY+"="+city+"|"
                            +Constants.FIELD_SEX+"="+sex+"|";

            return new Tuple2<>(sessionId,fullAggrInf);
        });

        return sessionId2FullAggrInfoRDD;
    }

    /**
     * filter and do accumulator stats
     * @param sessionId2AggrInfoRDD
     * @param taskParam
     * @param sessionAggrStatAccumulator
     * @return <sessionId,<sessionId,searchKeywords,clickCategoryIds,age,professional,city,sex>>
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD sessionId2AggrInfoRDD
            , final JSONObject taskParam, final SessionAggrStatAccumulator sessionAggrStatAccumulator){

        String startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam,Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam,Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE +"="+startAge+"|" :"")
            +(endAge != null ? Constants.PARAM_END_AGE +"="+endAge+"|" :"")
                +(professionals != null ? Constants.PARAM_PROFESSIONALS +"="+professionals+"|" :"")
                +(cities != null ? Constants.PARAM_CITIES +"="+cities+"|" :"")
                +(sex != null ? Constants.PARAM_SEX +"="+sex+"|" :"")
                +(keywords != null ? Constants.PARAM_KEYWORDS +"="+keywords+"|" :"")
                +(categoryIds != null ? Constants.PARAM_CATEGORY_IDS +"="+categoryIds+"|" :"")
                ;
        if (_parameter.endsWith("|")){
            _parameter = _parameter.substring(0,_parameter.length()-1);
        }

        final String parameter = _parameter;


        JavaPairRDD filteredSessionId2AggrInfoRDD = sessionId2AggrInfoRDD.filter(
                (Function<Tuple2<String,String>,Boolean>) (Tuple2<String, String> tuple) ->{
            // step 1: get aggr info
            String aggrInfo = tuple._2;
            // step 2: filter by conditions
            // age (startAge, endAge)
            if(!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)) {
                return false;
            }

            // professionals
            if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,parameter,Constants.PARAM_PROFESSIONALS)) {
                return false;
            }
            //cities
            if(!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,parameter,Constants.PARAM_CITIES)) {
                return false;
            }
            //sex
            if(!ValidUtils.equal(aggrInfo,Constants.FIELD_SEX,parameter,Constants.PARAM_SEX)) {
                return false;
            }
            //keywords
            if(!ValidUtils.in(aggrInfo,Constants.FIELD_SEARCH_KEYWORDS,parameter,Constants.PARAM_KEYWORDS)) {
                return false;
            }
            //categoryIds
            if(!ValidUtils.equal(aggrInfo,Constants.FIELD_CATEGORY_ID,parameter,Constants.PARAM_CATEGORY_IDS)) {
                return false;
            }

            // all conditions passed
            // aggregate by visit_length and step_length
            sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
            // aggregate by visit_length and step_length
            Long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|"
                    ,Constants.FIELD_VISIT_LENGTH));
                    Long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|"
                    ,Constants.FIELD_STEP_LENGTH));
            // do group stats against visitLength
            if(visitLength>=0 && visitLength <=3){
                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
            } else if(visitLength>=4 && visitLength <=6){
                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
            } else if(visitLength>=7 && visitLength <=9){
                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
            } else if(visitLength>=10 && visitLength <=30){
                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
            } else if(visitLength>=31 && visitLength <=60){
                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
            } else if(visitLength>=1*60+1 && visitLength <=3*60){
                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
            } else if(visitLength>=3*60+1 && visitLength <=10*60){
                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
            } else if(visitLength>=10*60+1 && visitLength <=30*60){
                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
            } else if(visitLength>=30*60+1){
                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
            }

            // do stats against stepLength
            if(stepLength>=1 && stepLength <=3){
                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
            } else if(stepLength>=4 && stepLength <=6){
                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
            } else if(stepLength>=7 && stepLength <=9){
                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
            } else if(stepLength>=10 && stepLength <=30){
                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
            } else if(stepLength>=31 && stepLength <=60){
                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
            } else if(stepLength>=61 ){
                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
            }

            return true;
        });
        return filteredSessionId2AggrInfoRDD;
    }

    /**
     * get detail information for eligible sessions
     * @param sessionId2AggrInfoRDD
     * @param sessionId2ActionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionId2DetailRDD(
            JavaPairRDD<String, String> sessionId2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionId2ActionRDD
    ){
        JavaPairRDD<String, Row> sessionId2DetailRDD = sessionId2AggrInfoRDD.join(sessionId2ActionRDD)
                .mapToPair(item -> new Tuple2<>(item._1, item._2._2));
        return sessionId2DetailRDD;
    }
    /**
     * extract sessions randomly
     * @param spark
     * @param taskId
     * @param sessionId2AggrInfoRDD
     * @param sessionId2ActionRDD
     */
    private static void randomExtractSession(SparkSession spark, final long taskId
            , JavaPairRDD<String, String> sessionId2AggrInfoRDD,JavaPairRDD<String, Row> sessionId2ActionRDD) {
        // step 1. count sessions per hour per day. return <yyyy-MM-dd_HH, sessionId>
        JavaPairRDD<String, String> time2SessionIdRDD = sessionId2AggrInfoRDD.mapToPair(
                tuple -> {
                    String aggrInfo = tuple._2;
                    String startTime = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME);
                    String dateHour = DateUtils.getDateHour(startTime);
                    return new Tuple2<>(dateHour,aggrInfo);
                }
        );
        Map<String, Long> countMap = time2SessionIdRDD.countByKey();
        // step 2. use the algorithm of time-ratio-random-extraction to get indexes of the extracted sessions
        // per hour per day
        // step 2.1. transform <yyyy-MM-dd_HH,count> to <yyyy-MM-dd,<HH,count>>
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap();
        countMap.forEach((dateHour,count) ->{
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap == null){
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date,hourCountMap);
            }
            hourCountMap.put(hour,count);
        });

        // step 2.2. implement the algorithm of time-ratio-random-extraction
        int extractNumberPerDay = 100/dateHourCountMap.size();
        // <date,<hour,[3,5,20]>>
        // broadcast
        final Map<String, Map<String,List<Integer>>> dateHourExtractMap = new HashMap<>();

        Random random = new Random();
        dateHourCountMap.forEach((date,hourCountMap)->{
            //calculate the session count on the date
            Long sessionCount = 0L;
            for (Long hourCount:hourCountMap.values()
                 ) {
                sessionCount += hourCount;
            }
            final Long sessionCountFinal = sessionCount;

            Map<String,List<Integer>> hourExtractMap = dateHourExtractMap.get(date);

            if(hourExtractMap == null){
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date,hourExtractMap);
            }

            // traverse each hour
            for (Map.Entry<String,Long> hourCountEntry: hourCountMap.entrySet()){
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();
                //System.out.println("hour: "+ hour + ",Count: "+ count);
                int hourExtractNumber = (int)((double)count/(double)sessionCountFinal*extractNumberPerDay);
                if(hourExtractNumber > count){
                    hourExtractNumber = (int)count;
                }
                // get random sessionId list for current hour

                //System.out.println("hourExtractMap == null? "+ (hourExtractMap == null ? "Yes":"No"));
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if(extractIndexList == null){
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour,extractIndexList);
                }

                // generate hourExtractNumber random session ids
                for(int i = 0; i< hourExtractNumber; i++){
                    int extractIndex= random.nextInt((int)count);
                    while(extractIndexList.contains(extractIndex)){
                        extractIndex= random.nextInt((int)count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        });

        // use fastutil collections to tune the performance
        Map<String, Map<String,IntList>> fastutilDateHourExtractMap = new HashMap<>();
        dateHourExtractMap.forEach((date,hourExtractMap)->{
            Map<String,IntList> fastutilHourExtractMap = new HashMap<>();
            hourExtractMap.forEach((hour,extractList)->{
                IntList fastutilExtractList = new IntArrayList();
                extractList.forEach(extractListEntry -> {
                    fastutilExtractList.add(extractListEntry);
                });
                fastutilHourExtractMap.put(hour,fastutilExtractList);
            });
            fastutilDateHourExtractMap.put(date,fastutilHourExtractMap);
        });
        // broadcast the map
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final Broadcast<Map<String, Map<String,IntList>>> dateHourExtractMapBroadcast =
                sc.broadcast(fastutilDateHourExtractMap);
        // step 3: extract sessions from time2SessionIdRDD according to session ids extracted randomly
        // do groupBy to get <dateHour, (session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2SessionsRDD = time2SessionIdRDD.groupByKey();

        // get specific sessions and write them to mysql table random_extract_session
        // place the session ids into extractSessionsRDD
        // and then use the sessionIds to join sessionId2AggrInfoRDD to get detail information for extracted
        // sessions and write them to mysql table session_detail
        JavaPairRDD<String, String> extractSessionsRDD = time2SessionsRDD.flatMapToPair(tuple->{
            List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();

            String dateHour = tuple._1;
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            Map<String, Map<String,IntList>> dateHourExtractMap0 = dateHourExtractMapBroadcast.getValue();
            IntList extractIndexList = dateHourExtractMap0.get(date).get(hour);

            ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

            int index = 0;
            Iterator<String> iterator = tuple._2.iterator();
            while(iterator.hasNext()){
                String sessionAggrInfo = iterator.next();
                if(extractIndexList.contains(index)){
                    // write session aggr info to table using DAO
                    SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                    sessionRandomExtract.setTaskId(taskId);
                    String sessionId = StringUtils.getFieldFromConcatString(
                            sessionAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                    sessionRandomExtract.setSessionId(sessionId);
                    sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                            sessionAggrInfo,"\\|",Constants.FIELD_START_TIME));
                    sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                            sessionAggrInfo,"\\|",Constants.FIELD_SEARCH_KEYWORDS));
                    sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                            sessionAggrInfo,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS));
                    // write to table
                    sessionRandomExtractDAO.insert(sessionRandomExtract);
                    // add sessionId to the list
                    extractSessionIds.add(new Tuple2<>(sessionId,sessionId));
                }
                index++;
            }
            return extractSessionIds.iterator();
        });

        // step 4. get session details by joining extractSessionsRDD and sessionId2ActionRDD
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionsRDD.join(sessionId2ActionRDD);

        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();

        extractSessionDetailRDD.foreachPartition(itemIterator ->{
            List<SessionDetail> sessionDetails = new ArrayList<>();
            while(itemIterator.hasNext()){
                Tuple2<String, Tuple2<String, Row>> item = itemIterator.next();
                Row row = item._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskId(taskId);
                sessionDetail.setUserId(row.getLong(1));
                sessionDetail.setSessionId(row.getString(2));
                sessionDetail.setPageId(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.isNullAt(6)? null: row.getLong(6));
                sessionDetail.setClickProductId(row.isNullAt(7)? null: row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
                sessionDetails.add(sessionDetail);
            }
            sessionDetailDAO.insertBatch(sessionDetails);
        });

    }

    /**
     * calculate ratios of session groups and write the the result into mysql
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskId) {
        long sessionCount = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.SESSION_COUNT));
        long time_period_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1s_3s));
        long time_period_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_4s_6s));
        long time_period_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_7s_9s));
        long time_period_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10s_30s));
        long time_period_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30s_60s));
        long time_period_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1m_3m));
        long time_period_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_3m_10m));
        long time_period_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10m_30m));
        long time_period_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30m));

        long step_period_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_1_3));
        long step_period_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_4_6));
        long step_period_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_7_9));
        long step_period_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_10_30));
        long step_period_30_60= Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_30_60));
        long step_period_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_60));
        
        //to avoid DividedByZero exception
        double sessionCount2 = (sessionCount ==0?1:sessionCount);
        double time_period_1s_3s_ratio = NumberUtils.formatDouble(time_period_1s_3s/sessionCount2,2);
        double time_period_4s_6s_ratio = NumberUtils.formatDouble(time_period_4s_6s/sessionCount2,2);
        double time_period_7s_9s_ratio = NumberUtils.formatDouble(time_period_7s_9s/sessionCount2,2);
        double time_period_10s_30s_ratio = NumberUtils.formatDouble(time_period_10s_30s/sessionCount2,2);
        double time_period_30s_60s_ratio = NumberUtils.formatDouble(time_period_30s_60s/sessionCount2,2);
        double time_period_1m_3m_ratio = NumberUtils.formatDouble(time_period_1m_3m/sessionCount2,2);
        double time_period_3m_10m_ratio = NumberUtils.formatDouble(time_period_3m_10m/sessionCount2,2);
        double time_period_10m_30m_ratio = NumberUtils.formatDouble(time_period_10m_30m/sessionCount2,2);
        double time_period_30m_ratio = NumberUtils.formatDouble(time_period_30m/sessionCount2,2);

        double step_period_1_3_ratio = NumberUtils.formatDouble(step_period_1_3/sessionCount2,2);
        double step_period_4_6_ratio = NumberUtils.formatDouble(step_period_4_6/sessionCount2,2);
        double step_period_7_9_ratio = NumberUtils.formatDouble(step_period_7_9/sessionCount2,2);
        double step_period_10_30_ratio = NumberUtils.formatDouble(step_period_10_30/sessionCount2,2);
        double step_period_30_60_ratio = NumberUtils.formatDouble(step_period_30_60/sessionCount2,2);
        double step_period_60_ratio = NumberUtils.formatDouble(step_period_60/sessionCount2,2);

        //write to mysql
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskId(taskId);
        sessionAggrStat.setSession_count(sessionCount);
        sessionAggrStat.setTime_period_1s_3s_ratio(time_period_1s_3s_ratio);
        sessionAggrStat.setTime_period_4s_6s_ratio(time_period_4s_6s_ratio);
        sessionAggrStat.setTime_period_7s_9s_ratio(time_period_7s_9s_ratio);
        sessionAggrStat.setTime_period_10s_30s_ratio(time_period_10s_30s_ratio);
        sessionAggrStat.setTime_period_30s_60s_ratio(time_period_30s_60s_ratio);
        sessionAggrStat.setTime_period_1m_3m_ratio(time_period_1m_3m_ratio);
        sessionAggrStat.setTime_period_3m_10m_ratio(time_period_3m_10m_ratio);
        sessionAggrStat.setTime_period_10m_30m_ratio(time_period_10m_30m_ratio);
        sessionAggrStat.setTime_period_30m_ratio(time_period_30m_ratio);

        sessionAggrStat.setStep_period_1_3_ratio(step_period_1_3_ratio);
        sessionAggrStat.setStep_period_4_6_ratio(step_period_4_6_ratio);
        sessionAggrStat.setStep_period_7_9_ratio(step_period_7_9_ratio);
        sessionAggrStat.setStep_period_10_30_ratio(step_period_10_30_ratio);
        sessionAggrStat.setStep_period_30_60_ratio(step_period_30_60_ratio);
        sessionAggrStat.setStep_period_60_ratio(step_period_60_ratio);

        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    /**
     * get top 10 categories
     * @param sessionId2DetailRDD
     */
    private static List<Tuple2<CategorySortKey,String>> getTop10Category(Long taskId, JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /**
         * step 1. get all categories accessed by eligible sessions
          */

        // get all categories accessed (clicked, ordered, or paid)
        // (categoryId, categoryId)
        JavaPairRDD<Long,Long> categoryIdRDD = sessionId2DetailRDD.flatMapToPair(tuple ->{
            Row row  = tuple._2;
            List<Tuple2<Long,Long>> list = new ArrayList<>();
            // click_category_id
            if(!row.isNullAt(6)){
                Long clickCategoryId = row.getLong(6);
                list.add(new Tuple2<>(clickCategoryId,clickCategoryId));
            }
            // order_category_ids
            String orderCategoryIds = row.getString(8);
            if(orderCategoryIds != null){
                String[] orderCategoryIdsSplitted = orderCategoryIds.split(",");
                for(String orderCategoryId: orderCategoryIdsSplitted){
                    list.add(new Tuple2<>(Long.valueOf(orderCategoryId),Long.valueOf(orderCategoryId)));
                }
            }
            // pay_category_ids
            String payCategoryIds = row.getString(10);
            if(payCategoryIds != null){
                String[] payCategoryIdsSplitted = payCategoryIds.split(",");
                for(String payCategoryId: payCategoryIdsSplitted){
                    list.add(new Tuple2<>(Long.valueOf(payCategoryId),Long.valueOf(payCategoryId)));
                }
            }
            return list.iterator();

        });
        categoryIdRDD = categoryIdRDD.distinct();
        /**
         * step 2. calculate each category's count of click, order, and payment
         */
        // 1. get each category's click count
        JavaPairRDD<Long,Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionId2DetailRDD);

        // 2. get each category's order count
        JavaPairRDD<Long,Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionId2DetailRDD);

        // 3. get each category's payment count
        JavaPairRDD<Long,Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionId2DetailRDD);

        /**
         * step 3. join categoryIdRDD and the 3 count RDDs
         */
        JavaPairRDD<Long,String> categoryId2CountRDD = joinCategoryIdAndData(
                categoryIdRDD,
                clickCategoryId2CountRDD,
                orderCategoryId2CountRDD,
                payCategoryId2CountRDD);

        /**
         * step 4. define secondary-sort key
          */


        /**
         *  step 5. map the above RDD to that with format of (CategorySortKey, info) and do sorting
         */
        JavaPairRDD<CategorySortKey,String> sortKey2CountRDD = categoryId2CountRDD.mapToPair(tuple -> {
            String countInfo = tuple._2;
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo,"\\|",Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo,"\\|",Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo,"\\|",Constants.FIELD_PAY_COUNT));

            CategorySortKey categorySortKey = new CategorySortKey(clickCount,orderCount,payCount);

            return new Tuple2<>(categorySortKey,countInfo);
        });

        JavaPairRDD<CategorySortKey,String> sortedCategoryCountRDD = sortKey2CountRDD.sortByKey(false);

        /**
         *  step 6. take(10) and then write to MySQL
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        List<Tuple2<CategorySortKey,String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        for (Tuple2<CategorySortKey,String> item: top10CategoryList
             ) {
            CategorySortKey categorySortKey = item._1;
            String countInfo = item._2;
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo,"\\|",Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo,"\\|",Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo,"\\|",Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo,"\\|",Constants.FIELD_PAY_COUNT));

            Top10Category top10Category = new Top10Category();
            top10Category.setTaskId(taskId);
            top10Category.setCategoryId(Long.valueOf(categoryId));
            top10Category.setClickCount(clickCount);
            top10Category.setOrderCount(orderCount);
            top10Category.setPayCount(payCount);

            top10CategoryDAO.insert(top10Category);
        }
        return top10CategoryList;
    }


    private static JavaPairRDD<Long,Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionId2DetailRDD.filter(tuple -> !tuple._2.isNullAt(6));
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(tuple ->
                new Tuple2<>(tuple._2.getLong(6),1L));
        JavaPairRDD<String, Long> mappedClickCategoryIdRDD = clickCategoryIdRDD.mapToPair(tuple ->{
            Random random = new Random();
            return new Tuple2<>(random.nextInt(10) + "_"+tuple._1,tuple._2);
        });
        JavaPairRDD<String, Long> firstAggrRDD = mappedClickCategoryIdRDD.reduceByKey((x,y)-> x +y);
        JavaPairRDD<Long, Long> restoredRDD = firstAggrRDD.mapToPair(tuple -> {
            Long categoryId = Long.valueOf(tuple._1.split("_")[1]);
            return new Tuple2<>(categoryId,tuple._2);
        });

        return restoredRDD.reduceByKey((x,y)-> x +y);

    }
    private static JavaPairRDD<Long,Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionId2DetailRDD.filter(tuple -> !tuple._2.isNullAt(8));
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(tuple -> {
            Row row  = tuple._2;
            List<Tuple2<Long,Long>> list = new ArrayList<>();
            String orderCategoryIds = row.getString(8);
            String[] orderCategoryIdsSplitted = orderCategoryIds.split(",");
            for(String orderCategoryId: orderCategoryIdsSplitted){
                list.add(new Tuple2<>(Long.valueOf(orderCategoryId),1L));
            }
            return list.iterator();
        });
        return orderCategoryIdRDD.reduceByKey((x,y)-> x +y);

    }

    private static JavaPairRDD<Long,Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionId2DetailRDD.filter(tuple -> !tuple._2.isNullAt(10));
        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(tuple -> {
            Row row  = tuple._2;
            List<Tuple2<Long,Long>> list = new ArrayList<>();
            String payCategoryIds = row.getString(10);
            String[] payCategoryIdsSplitted = payCategoryIds.split(",");
            for(String payCategoryId: payCategoryIdsSplitted){
                list.add(new Tuple2<>(Long.valueOf(payCategoryId),1L));
            }
            return list.iterator();
        });
        return payCategoryIdRDD.reduceByKey((x,y)-> x +y);

    }

    /**
     * join categoryIdRDD and 3 count RDDs : LeftOuterJoin
     * @param categoryIdRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return (categoryId, categoryId=x|clickCount=1|orderCount=1|payCount=0)
     */
    private static JavaPairRDD<Long, String> joinCategoryIdAndData(
            JavaPairRDD<Long, Long> categoryIdRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        // 1. left join click
        // 2. left join order
        // 3. left join pay
        JavaPairRDD<Long, String> tmpMapRDD = categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD).mapToPair(tuple -> {
            Long categoryId = tuple._1;
            Optional<Long> clickCountOptional = tuple._2._2;
            Long clickCount = 0L;
            if(clickCountOptional.isPresent()){
                clickCount = clickCountOptional.get();
            }
            String value = Constants.FIELD_CATEGORY_ID + "="+ categoryId
                    + "|" + Constants.FIELD_CLICK_COUNT + "="+ clickCount;
            return new Tuple2<>(categoryId,value);
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(tuple -> {
            Long categoryId = tuple._1;
            String value = tuple._2._1;
            Optional<Long> orderCountOptional = tuple._2._2;
            Long orderCount = 0L;
            if(orderCountOptional.isPresent()){
                orderCount = orderCountOptional.get();
            }
            value = value
                    + "|" + Constants.FIELD_ORDER_COUNT + "="+ orderCount;
            return new Tuple2<>(categoryId,value);
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(tuple -> {
            Long categoryId = tuple._1;
            String value = tuple._2._1;
            Optional<Long> payCountOptional = tuple._2._2;
            Long payCount = 0L;
            if(payCountOptional.isPresent()){
                payCount = payCountOptional.get();
            }
            value = value
                    + "|" + Constants.FIELD_PAY_COUNT + "="+ payCount;
            return new Tuple2<>(categoryId,value);
        });
        return tmpMapRDD;
    }

    /**
     * get top 10 active sessions
     * @param taskId
     * @param sessionId2DetailRDD
     */
    private static void getTop10CategorySession(SparkSession spark, long taskId,
                                                List<Tuple2<CategorySortKey,String>> top10CategoryList,
                                                JavaPairRDD<String, Row> sessionId2DetailRDD) {
        /**
         * step 1. generate an RDD from top 10 category list
         */
        List<Row> top10CategoryIdList = new ArrayList<>();
        for (Tuple2<CategorySortKey,String> category:  top10CategoryList
             ) {
            Long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(
                    category._2,"\\|",Constants.FIELD_CATEGORY_ID
            ));
            Row row = RowFactory.create(categoryId);
            top10CategoryIdList.add(row);
        }
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("category_id", DataTypes.LongType, true)));
        Dataset<Row> top10CategoryDataSet = spark.createDataFrame(top10CategoryIdList, schema);
        JavaPairRDD<Long, Long> top10CategoryRDD = top10CategoryDataSet.toJavaRDD().mapToPair(
                row -> new Tuple2<>(row.getLong(0),row.getLong(0)));

        /**
         * step 2. calculate session's click count for each category
         */
        JavaPairRDD<String, Iterable<Row>> sessionId2DetailsRDD = sessionId2DetailRDD.groupByKey();

        JavaPairRDD<Long,String> category2SessionCountRDD = sessionId2DetailsRDD.flatMapToPair(tuple ->{
            String sessionId = tuple._1;
            Iterator<Row> iterator = tuple._2.iterator();
            Map<Long, Long> categoryCountMap = new HashMap<>();
            while(iterator.hasNext()){
                Row row = iterator.next();
                if(!row.isNullAt(6)){
                    Long categoryId = row.getLong(6);
                    Long count = categoryCountMap.get(categoryId);
                    if(count == null){
                        count = 0L;
                    }
                    count ++;
                    categoryCountMap.put(categoryId,count);
                }
            }
            // return: <categoryId, "sessionId,count">
            List<Tuple2<Long,String>> category2SessionCountList = new ArrayList<>();
            categoryCountMap.forEach((categoryId,count) -> {
                category2SessionCountList.add(new Tuple2<>(categoryId,sessionId+","+count));
            });
            return category2SessionCountList.iterator();
        });

        //get session's click count for each category
        // format: <categoryId, "sessionId,count">
        JavaPairRDD<Long,String> top10CategorySessionCountRDD = top10CategoryRDD.join(category2SessionCountRDD)
                .mapToPair(tuple -> new Tuple2<>(tuple._1,tuple._2._2));

        /**
         * step 3. get top 10 sessions for each category
         */
        //group by category
        JavaPairRDD<Long,Iterable<String>> top10Category2SessionCountRDD = top10CategorySessionCountRDD.groupByKey();
        //<sessionId, sessionId> and write top 10 category session into mysql table
        JavaPairRDD<String,String> top10CateGorySessionRDD = top10Category2SessionCountRDD.flatMapToPair(
                tuple ->{
                    Long categoryId = tuple._1;
                    Iterator<String> iterator = tuple._2.iterator();
                    String[] sessionCounts = new String[10];
                    while(iterator.hasNext()){
                        String sessionCount = iterator.next();
                        Long count = Long.valueOf(sessionCount.split(",")[1]);
                        for(int i =0; i< sessionCounts.length; i++){
                            if(sessionCounts[i]== null){
                                sessionCounts[i] = sessionCount;
                                break;
                            } else {
                                Long count1 = Long.valueOf(sessionCounts[i].split(",")[1]);
                                if(count > count1) {
                                    for(int j =sessionCounts.length-1; j> i; j--) {
                                        sessionCounts[j] = sessionCounts[j-1];
                                    }
                                    sessionCounts[i] = sessionCount;
                                    break;
                                }
                            }
                        }

                    }
                    //write sessionCounts to mysql table
                    List<Tuple2<String, String>> list = new ArrayList<>();
                    ITop10CategorySessionDAO top10CategorySessionDAO = DAOFactory.getTop10CategorySessionDAO();
                    for(int i =0; i< sessionCounts.length; i++){
                        if(sessionCounts[i]== null){
                            break;
                        } else {
                            String sessionId = sessionCounts[i].split(",")[0];
                            Long clickCount = Long.valueOf(sessionCounts[i].split(",")[1]);
                            Top10CategorySession top10CategorySession = new Top10CategorySession();
                            top10CategorySession.setTaskId(taskId);
                            top10CategorySession.setCategoryId(categoryId);
                            top10CategorySession.setSessionId(sessionId);
                            top10CategorySession.setClickCount(clickCount);
                            top10CategorySessionDAO.insert(top10CategorySession);

                            // add to list
                            list.add(new Tuple2<>(sessionId, sessionId));
                        }
                    }
                return list.iterator();
                }
        );

        /**
         * step 4. get session details and write them to mysql table
         */
        JavaPairRDD<String, Tuple2<String, Row>> top10SessionDetailRDD =
                top10CateGorySessionRDD.join(sessionId2DetailRDD);

        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();

        top10SessionDetailRDD.foreachPartition(itemList ->{
            List<SessionDetail> sessionDetails = new ArrayList<>();
            while(itemList.hasNext()){
                Tuple2<String, Tuple2<String, Row>> item = itemList.next();
                Row row = item._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskId(taskId);
                sessionDetail.setUserId(row.getLong(1));
                sessionDetail.setSessionId(row.getString(2));
                sessionDetail.setPageId(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.isNullAt(6)? null: row.getLong(6));
                sessionDetail.setClickProductId(row.isNullAt(7)? null: row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
                sessionDetails.add(sessionDetail);
            }
            sessionDetailDAO.insertBatch(sessionDetails);
        });


    }

}
