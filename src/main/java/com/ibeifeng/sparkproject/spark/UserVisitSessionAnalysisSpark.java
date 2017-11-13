package com.ibeifeng.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.impl.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.StringUtils;
import com.ibeifeng.sparkproject.util.ValidUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;
import scala.xml.PrettyPrinter;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

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
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        long taskid = ParamUtils.getTaskIdFromArgs(args);


        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionRDD = getActionRDD(spark,taskParam);
        System.out.println("#actionRDD: " + actionRDD.count());
        //<sessionid,<sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex>>
        JavaPairRDD<String,String> sessionid2AggrInfoRDD =
                aggregateBySession(spark,actionRDD);
        System.out.println(sessionid2AggrInfoRDD.count());
        sessionid2AggrInfoRDD.take(10).forEach(tuple -> System.out.println(tuple._2));

        JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD =
                filterSession(sessionid2AggrInfoRDD,taskParam);
        System.out.println(filteredSessionid2AggrInfoRDD.count());
        filteredSessionid2AggrInfoRDD.takeSample(false,10).forEach(tuple -> System.out.println(tuple._2));


        spark.stop();
    }

    private static void mockData(SparkSession spark) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            MockData.mock(spark);
        }
    }

    private static SQLContext getSQLContext(SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    private static JavaRDD<Row> getActionRDD(SparkSession spark, JSONObject taskParam){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where date >='"+startDate+"' and date <= '"+endDate+"'";
        Dataset<Row> actionDF = spark.sql(sql);
        return actionDF.javaRDD();
    }

    private static JavaPairRDD<String,String> aggregateBySession(SparkSession spark, JavaRDD<Row> actionRDD){
        //map to <sessionID, Row>
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
                (PairFunction<Row, String, Row>) row -> new Tuple2<>(row.getString(2),row));
        //debug
        //System.out.println("#sessionid2ActionRDD:"+ sessionid2ActionRDD.count());
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD =  sessionid2ActionRDD.groupByKey();
        //debug
        //System.out.println("#sessionid2ActionsRDD:"+ sessionid2ActionRDD.count());

        //<userid, partAggrInfo(sessionid, searchKeywords, clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD =  sessionid2ActionsRDD.mapToPair(
                (PairFunction<Tuple2<String,Iterable<Row>>, Long, String> ) tuple -> {
                    String sessionid = tuple._1;
                    Long userid = null;
                    Iterator<Row> iterator = tuple._2.iterator();
                    StringBuffer searchKeywordsBuffer = new StringBuffer("");
                    StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                    while(iterator.hasNext()){
                        Row row = iterator.next();
                        String searchKeyword = row.getString(5);

                        Long clickCategoryId = null;
                        try {
                            clickCategoryId = row.getLong(6);
                        } catch (Exception e) {
                            //e.printStackTrace();
                            System.out.println(row.toString());
                        }
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

                    }
                    String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                    String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                    // return <userid, partAggrInfo>

                    String partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionid
                            + (StringUtils.isEmpty(searchKeywords)? "" : "|"+Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeywords)
                            + (StringUtils.isEmpty(clickCategoryIds)? "" : "|"+Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds);
                    return new Tuple2<>(userid,partAggrInfo);
                });
        //debug
        //System.out.println(userid2PartAggrInfoRDD.count());


        String sql = "select user_id,username,name,age,professional,city,sex from user_info";
        JavaRDD<Row> userInfoRDD = spark.sql(sql).toJavaRDD();
        JavaPairRDD<Long,Row> userid2InfoRDD = userInfoRDD.mapToPair((PairFunction<Row,Long,Row>) (Row row) ->
                new Tuple2<>(row.getLong(0),row)) ;
        //debug
//        userid2InfoRDD.takeSample(false,10).forEach(action -> System.out.println(action._1
//                + ", info: "+action._2.toString()
//        ));

        //join sessionAggr and user
        // return <userid, <partAggrInfo, userFullInfo>>
        JavaPairRDD<Long,Tuple2<String,Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);
        //debug
        //System.out.println(userid2FullInfoRDD.count());
//        userid2FullInfoRDD.takeSample(false,10).forEach(action -> System.out.println(
//                "userid: "+ action._1
//                + ", aggr: "+action._2._1
//                + ", info: "+action._2._2.toString()
//        ));

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

    private static JavaPairRDD<String, String> filterSession(JavaPairRDD sessionid2AggrInfoRDD, final JSONObject taskParam){

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
                (Function<Tuple2<String,String>,Boolean>) (tuple) ->{
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
            return true;
        });
        return filteredSessionid2AggrInfoRDD;
    }
}
