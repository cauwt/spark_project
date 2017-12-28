package com.ibeifeng.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.*;
import com.ibeifeng.sparkproject.util.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
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

        mockData(spark);

        long taskid = ParamUtils.getTaskIdFromArgs(args);


        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionRDD = getActionRDD(spark,taskParam);
        //<sessionid,<sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex>>
        JavaPairRDD<String,String> sessionid2AggrInfoRDD =
                aggregateBySession(spark,actionRDD);

        //refactor, filter and do stats
        SessionAggrStatAccumulator sessionAggrAccumulator = new SessionAggrStatAccumulator();
        spark.sparkContext().register(sessionAggrAccumulator);
        //<sessionid,<sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex>>
        JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD =
                filterSessionAndAggrStat(sessionid2AggrInfoRDD,taskParam,sessionAggrAccumulator);

        // extract sessions randomly
        JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);



        // trigger spark job
        System.out.println(filteredSessionid2AggrInfoRDD.count());
        // calculate ratios for session groups defined by visit length and step length
        calculateAndPersistAggrStat(sessionAggrAccumulator.value(), task.getTaskid());
        randomExtractSession(task.getTaskid()
                , filteredSessionid2AggrInfoRDD
                , sessionid2ActionRDD);


        //function 3: get top 10 categories most clicked, ordered and paid.
        getTop10Category(taskid, filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);

        spark.stop();
    }



    /**
     * mock data : user, product, and visict action
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
     * @return action info: date,user_id,sessionid,...
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
     * get map of sessionid to action details
     * @param actionRDD
     * @return sessionid, row(session details)
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(row -> new Tuple2<>(row.getString(2),row));
    }

    /**
     * do sessiton aggregration
     * @param spark
     * @param actionRDD
     * @return <sessionid,<sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex>>
     */
    private static JavaPairRDD<String,String> aggregateBySession(SparkSession spark, JavaRDD<Row> actionRDD){
        //map to <sessionID, Row>
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
                (PairFunction<Row, String, Row>) row -> new Tuple2<>(row.getString(2),row));
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD =  sessionid2ActionRDD.groupByKey();

        //<userid, partAggrInfo(sessionid, searchKeywords, clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD =  sessionid2ActionsRDD.mapToPair(
                (PairFunction<Tuple2<String,Iterable<Row>>, Long, String> ) tuple -> {
                    String sessionid = tuple._1;
                    Iterator<Row> iterator = tuple._2.iterator();
                    StringBuffer searchKeywordsBuffer = new StringBuffer("");
                    StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                    Long userid = null;
                    Date startTime = null;
                    Date endTime = null;
                    int stepLength = 0;

                    while(iterator.hasNext()){
                        Row row = iterator.next();
                        String searchKeyword = row.getString(5);

                        Long clickCategoryId = row.isNullAt(6)?null:row.getLong(6);
                        if(userid == null){
                            userid = row.getLong(1);
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

                    // return <userid, partAggrInfo>

                    String partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionid
                            + "|"+Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeywords
                            + "|"+Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds
                            + "|"+Constants.FIELD_VISIT_LENGTH +"="+visitLength
                            + "|"+Constants.FIELD_STEP_LENGTH +"="+stepLength
                            + "|"+Constants.FIELD_START_TIME +"="+DateUtils.formatTime(startTime);
                    return new Tuple2<>(userid,partAggrInfo);
                });


        String sql = "select user_id,username,name,age,professional,city,sex from user_info";
        JavaRDD<Row> userInfoRDD = spark.sql(sql).toJavaRDD();
        JavaPairRDD<Long,Row> userid2InfoRDD = userInfoRDD.mapToPair((PairFunction<Row,Long,Row>) (Row row) ->
                new Tuple2<>(row.getLong(0),row)) ;

        //join sessionAggr and user
        // return <userid, <partAggrInfo, userFullInfo>>
        JavaPairRDD<Long,Tuple2<String,Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        //return <sessionid,fullAggrInfo(age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
                (PairFunction<Tuple2<Long,Tuple2<String,Row>>,String,String>) (tuple) ->{
                    String partAggrInfo = tuple._2._1;
                    Row row = tuple._2._2;
                    String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                    int age = row.getInt(3);
                    String professional = row.getString(4);
                    String city = row.getString(5);
                    String sex = row.getString(6);

                    String fullAggrInf = partAggrInfo +"|"
                            +Constants.FIELD_AGE+"="+age+"|"
                            +Constants.FIELD_PROFESSIONAL+"="+professional+"|"
                            +Constants.FIELD_CITY+"="+city+"|"
                            +Constants.FIELD_SEX+"="+sex+"|";

            return new Tuple2<>(sessionid,fullAggrInf);
        });

        return sessionid2FullAggrInfoRDD;
    }

    /**
     * filter and do accumulator stats
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @param sessionAggrStatAccumulator
     * @return <sessionid,<sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex>>
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD sessionid2AggrInfoRDD
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


        JavaPairRDD filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
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
        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * extract sessions randomly
     * @param sessionid2AggrInfoRDD
     * @return (yyyy-MM-dd_HH, aggrInfo)
     */
    private static void randomExtractSession(final long taskid
            , JavaPairRDD<String, String> sessionid2AggrInfoRDD,JavaPairRDD<String, Row> sessionid2ActionRDD) {
        // step 1. count sessions per hour per day. return <yyyy-MM-dd_HH, sessionid>
        JavaPairRDD<String, String> time2SessionidRDD = sessionid2AggrInfoRDD.mapToPair(
                tuple -> {
                    String aggrInfo = tuple._2;
                    String startTime = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME);
                    String dateHour = DateUtils.getDateHour(startTime);
                    return new Tuple2<>(dateHour,aggrInfo);
                }
        );
        Map<String, Long> countMap = time2SessionidRDD.countByKey();
        // step 2. use the algorithm of time-ratio-random-extraction to get indexes of the extracted sessions
        // per hour per day
        // step 2.1. transform <yyyy-MM-dd_HH,count> to <yyyy-MM-dd,<HH,count>>
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap();
        Random random = new Random();
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
        final Map<String, Map<String,List<Integer>>> dateHourExtractMap = new HashMap<>();
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
                // get random sessionid list for current hour

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

        // step 3: extract sessions from time2SessionidRDD according to session ids extracted randomly
        // do groupBy to get <dateHour, (session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2SessionsRDD = time2SessionidRDD.groupByKey();

        // get specific sessions and write them to mysql table random_extract_session
        // place the session ids into extractSessionsRDD
        // and then use the sessionids to join sessionid2AggrInfoRDD to get detail information for extracted
        // sessions and write them to mysql table session_detail
        JavaPairRDD<String, String> extractSessionsRDD = time2SessionsRDD.flatMapToPair(tuple->{
            List<Tuple2<String, String>> extractSessionids = new ArrayList<>();

            String dateHour = tuple._1;
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

            ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

            int index = 0;
            Iterator<String> iterator = tuple._2.iterator();
            while(iterator.hasNext()){
                String sessionAggrInfo = iterator.next();
                if(extractIndexList.contains(index)){
                    // write session aggr info to table using DAO
                    SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                    sessionRandomExtract.setTaskid(taskid);
                    String sessionid = StringUtils.getFieldFromConcatString(
                            sessionAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                    sessionRandomExtract.setSessionid(sessionid);
                    sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                            sessionAggrInfo,"\\|",Constants.FIELD_START_TIME));
                    sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                            sessionAggrInfo,"\\|",Constants.FIELD_SEARCH_KEYWORDS));
                    sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                            sessionAggrInfo,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS));
                    // write to table
                    sessionRandomExtractDAO.insert(sessionRandomExtract);
                    // add sessionid to the list
                    extractSessionids.add(new Tuple2<String,String>(sessionid,sessionid));
                }
                index++;
            }
            return extractSessionids.iterator();
        });

        // step 4. get session details by joining extractSessionsRDD and sessionid2ActionRDD
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionsRDD.join(sessionid2ActionRDD);

        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();

        extractSessionDetailRDD.foreach(item ->{
            Row row = item._2._2;
            SessionDetail sessionDetail = new SessionDetail();
            sessionDetail.setTaskid(taskid);
            sessionDetail.setUserid(row.getLong(1));
            sessionDetail.setSessionid(row.getString(2));
            sessionDetail.setPageid(row.getLong(3));
            sessionDetail.setActionTime(row.getString(4));
            sessionDetail.setSearchKeyword(row.getString(5));
            sessionDetail.setClickCategoryId(row.isNullAt(6)? null: row.getLong(6));
            sessionDetail.setClickProductId(row.isNullAt(7)? null: row.getLong(7));
            sessionDetail.setOrderCategoryIds(row.getString(8));
            sessionDetail.setOrderProductIds(row.getString(9));
            sessionDetail.setPayCategoryIds(row.getString(10));
            sessionDetail.setPayProductIds(row.getString(11));
            sessionDetailDAO.insert(sessionDetail);
        });
    }

    /**
     * calculate ratios of session groups and write the the result into mysql
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
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
        sessionAggrStat.setTaskid(taskid);
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
     * get top 10 categoriies
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2ActionRDD
     */
    private static void getTop10Category(Long taskId, JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
                                         JavaPairRDD<String, Row> sessionid2ActionRDD) {
        /**
         * step 1. get all categories accessed by eligible sessions
          */

        // get details for eligible sessions
        JavaPairRDD<String, Row> sessionid2DetailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2ActionRDD)
                .mapToPair(item -> new Tuple2<>(item._1, item._2._2));
        // get all categories accessed (clicked, ordered, or paid)
        // (categoryid, categoryid)
        JavaPairRDD<Long,Long> categoryidRDD = sessionid2DetailRDD.flatMapToPair(tuple ->{
            Row row  = tuple._2;
            List<Tuple2<Long,Long>> list = new ArrayList<>();
            // click_category_id
            if(!row.isNullAt(6)){
                Long clickCategoryid = row.getLong(6);
                list.add(new Tuple2<>(clickCategoryid,clickCategoryid));
            }
            // order_category_ids
            String orderCategoryids = row.getString(8);
            if(orderCategoryids != null){
                String[] orderCategoryidsSplitted = orderCategoryids.split(",");
                for(String orderCategoryid: orderCategoryidsSplitted){
                    list.add(new Tuple2<>(Long.valueOf(orderCategoryid),Long.valueOf(orderCategoryid)));
                }
            }
            // pay_category_ids
            String payCategoryids = row.getString(10);
            if(payCategoryids != null){
                String[] payCategoryidsSplitted = payCategoryids.split(",");
                for(String payCategoryid: payCategoryidsSplitted){
                    list.add(new Tuple2<>(Long.valueOf(payCategoryid),Long.valueOf(payCategoryid)));
                }
            }
            return list.iterator();

        });

        /**
         * step 2. calculate each category's count of click, order, and payment
         */
        // 1. get each category's click count
        JavaPairRDD<Long,Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2DetailRDD);

        // 2. get each category's order count
        JavaPairRDD<Long,Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2DetailRDD);

        // 3. get each category's payment count
        JavaPairRDD<Long,Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2DetailRDD);

        /**
         * step 3. join categoryidRDD and the 3 count RDDs
         */
        JavaPairRDD<Long,String> categoryId2CountRDD = joinCategoryIdAndData(
                categoryidRDD,
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
    }


    private static JavaPairRDD<Long,Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2DetailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionid2DetailRDD.filter(tuple -> !tuple._2.isNullAt(6));
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(tuple ->
                new Tuple2<>(tuple._2.getLong(6),1L));
        return clickCategoryIdRDD.reduceByKey((x,y)-> x +y);

    }
    private static JavaPairRDD<Long,Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2DetailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionid2DetailRDD.filter(tuple -> !tuple._2.isNullAt(8));
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(tuple -> {
            Row row  = tuple._2;
            List<Tuple2<Long,Long>> list = new ArrayList<>();
            String orderCategoryids = row.getString(8);
            String[] orderCategoryidsSplitted = orderCategoryids.split(",");
            for(String orderCategoryid: orderCategoryidsSplitted){
                list.add(new Tuple2<>(Long.valueOf(orderCategoryid),1L));
            }
            return list.iterator();
        });
        return orderCategoryIdRDD.reduceByKey((x,y)-> x +y);

    }

    private static JavaPairRDD<Long,Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2DetailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionid2DetailRDD.filter(tuple -> !tuple._2.isNullAt(10));
        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(tuple -> {
            Row row  = tuple._2;
            List<Tuple2<Long,Long>> list = new ArrayList<>();
            String payCategoryids = row.getString(10);
            String[] payCategoryidsSplitted = payCategoryids.split(",");
            for(String payCategoryid: payCategoryidsSplitted){
                list.add(new Tuple2<>(Long.valueOf(payCategoryid),1L));
            }
            return list.iterator();
        });
        return payCategoryIdRDD.reduceByKey((x,y)-> x +y);

    }

    /**
     * join categoryidRDD and 3 count RDDs : LeftOuterJoin
     * @param categoryidRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return (categoryid, categoryid=x|clickCount=1|orderCount=1|payCount=0)
     */
    private static JavaPairRDD<Long, String> joinCategoryIdAndData(
            JavaPairRDD<Long, Long> categoryidRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        // 1. left join click
        // 2. left join order
        // 3. left join pay
        JavaPairRDD<Long, String> tmpMapRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD).mapToPair(tuple -> {
            Long categoryid = tuple._1;
            Optional<Long> clickCountOptional = tuple._2._2;
            Long clickCount = 0L;
            if(clickCountOptional.isPresent()){
                clickCount = clickCountOptional.get();
            }
            String value = Constants.FIELD_CATEGORY_ID + "="+ categoryid
                    + "|" + Constants.FIELD_CLICK_COUNT + "="+ clickCount;
            return new Tuple2<>(categoryid,value);
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(tuple -> {
            Long categoryid = tuple._1;
            String value = tuple._2._1;
            Optional<Long> orderCountOptional = tuple._2._2;
            Long orderCount = 0L;
            if(orderCountOptional.isPresent()){
                orderCount = orderCountOptional.get();
            }
            value = value
                    + "|" + Constants.FIELD_ORDER_COUNT + "="+ orderCount;
            return new Tuple2<>(categoryid,value);
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(tuple -> {
            Long categoryid = tuple._1;
            String value = tuple._2._1;
            Optional<Long> payCountOptional = tuple._2._2;
            Long payCount = 0L;
            if(payCountOptional.isPresent()){
                payCount = payCountOptional.get();
            }
            value = value
                    + "|" + Constants.FIELD_PAY_COUNT + "="+ payCount;
            return new Tuple2<>(categoryid,value);
        });
        return tmpMapRDD;
    }
}
