package com.cauwt.sparkproject.spark.ad;

import com.cauwt.sparkproject.conf.ConfigurationManager;
import com.cauwt.sparkproject.constant.Constants;
import com.cauwt.sparkproject.dao.*;
import com.cauwt.sparkproject.dao.factory.DAOFactory;
import com.cauwt.sparkproject.domain.*;
import com.cauwt.sparkproject.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

/**
 * Created by zkpk on 2/4/18.
 */
public class AdClickRealTimeStatSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
            .setIfMissing("spark.master", "local[*]")
                .setIfMissing("spark.appName","AdClickRealTimeStatSpark");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 1. build kafka consumer
        Map kafkaParams = new HashMap();
        kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
                ConfigurationManager.getProperties(Constants.KAFKA_METADATA_BROKER_LIST));
        kafkaParams.put(Constants.KAFKA_GROUP_ID,
                ConfigurationManager.getProperties(Constants.KAFKA_GROUP_ID));

        String topics = ConfigurationManager.getProperties(Constants.KAFKA_TOPICS);
        String[] topicArray = topics.split(",");
        Set<String> kafkaTopics = new HashSet<>();
        for (String topic: topicArray){
            kafkaTopics.add(topic);
        }

        // 2. consume kafka topic into spark streaming
        // format: val1, val2
        // val2: timestamp,province,city,user_id,ad_id
        JavaPairInputDStream<String, String> adRealTimeLogDStream =  KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                kafkaTopics);
        // 2.2. 使用transform操作，对每个batch RDD进行处理，都动态加载MySQL中的黑名单生成RDD，然后进行join后，过滤掉batch RDD中的黑名单用户的广告点击行为
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                filterByBlackList(adRealTimeLogDStream);
        // generate dynamic blacklist
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);


        // 6. 使用updateStateByKey操作，实时计算每天各省各城市各广告的点击量，并实时更新到MySQL
        JavaPairDStream<String, Long> adRealTimeStatDStream = calCulateRealTimeStat(filteredAdRealTimeLogDStream);

        // 7. 使用transform结合Spark SQL，统计每天各省份top3热门广告
        calculateProvinceTop3Ad(adRealTimeStatDStream);


        // 8. 使用window操作，对最近1小时滑动窗口内的数据，计算出各广告各分钟的点击量，并更新到MySQL中
        calculateAdClickCountByWindow(adRealTimeLogDStream);


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jssc.close();

    }

    /**
     * calculate ad click count by window
     * @param adRealTimeLogDStream
     */
    private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // use the original DStream since other streams are aggregated by days while we need one with minutes
        // map
        // src: <string, string(timestamp, province, city, userId, adId)>
        // target: <yyyyMMddHHmm_adId, clickCount>
        JavaPairDStream<String, Long> ds1 = adRealTimeLogDStream.mapToPair(tuple -> {
            String log = tuple._2;
            String[] logSplitted = log.split(",");
            String timestamp = logSplitted[0];
            Date date = new Date(Long.valueOf(timestamp));
            String timeMinuteKey = DateUtils.formatTimeMinute(date);
            String adId = logSplitted[4];
            String key = timeMinuteKey+"_"+adId;
            Long value = 1L;
            return new Tuple2<>(key,value);
        });
        // reduceByKeyWithWindows
        JavaPairDStream<String, Long> aggrDStream = ds1.reduceByKeyAndWindow(
                (x,y) -> x+y, Durations.minutes(60),Durations.seconds(60));

        // write to database
        aggrDStream.foreachRDD(rdd -> rdd.foreachPartition(partitionOfRecords -> {
            List<AdClickTrend> adClickTrends = new ArrayList<>();
            while(partitionOfRecords.hasNext()){
                Tuple2<String, Long> tuple = partitionOfRecords.next();
                String key = tuple._1;
                String[] keySplitted = key.split("_");
                String dateMinute = keySplitted[0];
                String date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0,8)));
                String hour = dateMinute.substring(8,10);
                String minute = dateMinute.substring(10);
                Long adId = Long.valueOf(keySplitted[1]);
                Long clickCount = tuple._2;

                AdClickTrend adClickTrend = new AdClickTrend();
                adClickTrend.setDate(date);
                adClickTrend.setHour(hour);
                adClickTrend.setMinute(minute);
                adClickTrend.setAdId(adId);
                adClickTrend.setClickCount(clickCount);

                adClickTrends.add(adClickTrend);
            }
            IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
            adClickTrendDAO.updateBatch(adClickTrends);
        }));
    }

    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
        // map
        // src: <yyyyMMdd_province_city_adId, clickCount>
        // target: <yyyyMMdd_province_adId, clickCount>
        JavaPairDStream<String, Long> ds1 = adRealTimeStatDStream.mapToPair(tuple ->{
            String key = tuple._1;
            Long clickCount = tuple._2;
            String[] keySplitted = key.split("_");
            String dateKey = keySplitted[0];
            String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey));
            String province = keySplitted[1];
            Long adId = Long.valueOf(keySplitted[3]);

            String newKey = date+"_"+province+"_"+adId;
            return new Tuple2<>(newKey,clickCount);
        }).reduceByKey((x,y) -> x+y);
        // transform: get top 3 using sparkSQL
        JavaDStream<Row> rowsDStream =  ds1.transform(rdd -> {
            JavaRDD<Row> rowsRDD = rdd.map(tuple -> {
                String key = tuple._1;
                String[] keySplitted = key.split("_");
                String dateKey = keySplitted[0];
                String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey));
                String province = keySplitted[1];
                Long adId = Long.valueOf(keySplitted[3]);
                Long clickCount = tuple._2;
                return RowFactory.create(date,province,adId,clickCount);
            });

            StructType schema = DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("date", DataTypes.StringType, true),
                    DataTypes.createStructField("province", DataTypes.StringType, true),
                    DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                    DataTypes.createStructField("click_count", DataTypes.LongType, true)
            ));
            SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
            Dataset<Row> ds = spark.createDataFrame(rowsRDD,schema);
            ds.createOrReplaceTempView("tmp_daily_ad_click_count_by_province");
            Dataset<Row> provinceTop3Dataset =  spark.sql(
                    "select date, province, ad_id, click_count from (" +
                            "select " +
                            "date, " +
                            "province, " +
                            "ad_id, " +
                            "click_count," +
                            "row_number() over(partition by date, province order by click_count desc) as rank" +
                            "from tmp_daily_ad_click_count_by_province )s" +
                            "where s.rank <=3;");
            return provinceTop3Dataset.toJavaRDD();
        });
        // write to database
        rowsDStream.foreachRDD(rdd -> rdd.foreachPartition(partitionOfRecords -> {
            List<AdProvinceTop3> adProvinceTop3s = new ArrayList<>();
            while (partitionOfRecords.hasNext()){
                Row row = partitionOfRecords.next();
                String date = row.getString(1);
                String province = row.getString(2);
                Long adId = row.getLong(3);
                Long clickCount = row.getLong(4);

                AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                adProvinceTop3.setDate(date);
                adProvinceTop3.setProvince(province);
                adProvinceTop3.setAdId(adId);
                adProvinceTop3.setClickCount(clickCount);

                adProvinceTop3s.add(adProvinceTop3);
            }
            IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
            adProvinceTop3DAO.updateBatch(adProvinceTop3s);
        }));
    }

    private static JavaPairDStream<String, Long> calCulateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // src: <string,string>
        // target: yyyyMMdd_province_city_adId, 1
        JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(tuple ->{
            String log = tuple._2;
            String[] logSplitted = log.split(",");
            String timestamp = logSplitted[0];
            Date date = new Date(Long.valueOf(timestamp));
            String dateKey = DateUtils.formatDateKey(date);
            String province = logSplitted[1];
            String city = logSplitted[2];
            String adId = logSplitted[4];
            String key = dateKey+"_"+province+"_"+city+"_"+adId;
            Long value = 1L;
            return new Tuple2<>(key,value);
        });
        // accumulate
        JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey((values, optional)-> {
            Long clickCount = 0L;
            if (optional.isPresent()){
                clickCount = optional.get();
            }
            for (Long value : values){
                clickCount += value;
            }
            return Optional.of(clickCount);
        });
        // write into database
        aggregatedDStream.foreachRDD(rdd -> rdd.foreachPartition(partitionOfRecords -> {
            List<AdStat> adStats = new ArrayList<>();
            while(partitionOfRecords.hasNext()){
                Tuple2<String, Long> tuple = partitionOfRecords.next();
                String key = tuple._1;
                Long clickCount = tuple._2;
                String[] keySplitted = key.split("_");
                String dateKey = keySplitted[0];
                String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey));
                String province = keySplitted[1];
                String city = keySplitted[2];
                Long adId = Long.valueOf(keySplitted[3]);

                AdStat adStat = new AdStat();
                adStat.setDate(date);
                adStat.setProvince(province);
                adStat.setCity(city);
                adStat.setAdId(adId);
                adStat.setClickCount(clickCount);
            }
            IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
            adStatDAO.updateBatch(adStats);
        }));
        return aggregatedDStream;
    }

    private static void generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 3. 实时计算各batch中的每天各用户对各广告的点击次数
        // src: timestamp,province,city,user_id,ad_id
        // tgt: yyyyMMdd_userid_adid, clickCount
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(tuple -> {
            String log = tuple._2;
            String[] logSplitted = log.split(",");
            String timestamp = logSplitted[0];
            Date date = new Date(Long.valueOf(timestamp));
            String dateKey = DateUtils.formatDateKey(date);
            String userId = logSplitted[3];
            String adId = logSplitted[4];
            String key = dateKey+"_"+userId+"_"+adId;
            Long value = 1L;
            return new Tuple2<>(key,value);
        });
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(
                (x,y) -> x + y);
        // 4. 使用高性能方式将每天各用户对各广告的点击次数写入MySQL中（更新）
        dailyUserAdClickCountDStream.foreachRDD( rdd -> rdd.foreachPartition(partitionOfRecords ->{
                final List<AdUserClickCount> adUserClickCounts = new ArrayList<>();
                while (partitionOfRecords.hasNext()){
                    Tuple2<String, Long> tuple = partitionOfRecords.next();

                    String[] keySplitted = tuple._1.split("_");
                    String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplitted[0]));
                    Long userId = Long.valueOf(keySplitted[1]);
                    Long adId = Long.valueOf(keySplitted[2]);
                    Long clickCount = tuple._2;

                    AdUserClickCount adUserClickCount = new AdUserClickCount();
                    adUserClickCount.setDate(date);
                    adUserClickCount.setUserId(userId);
                    adUserClickCount.setAdId(adId);
                    adUserClickCount.setClickCount(clickCount);

                    adUserClickCounts.add(adUserClickCount);
                }
                IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                adUserClickCountDAO.updateBatch(adUserClickCounts);

        }));
        // 5. 使用filter过滤出每天对某个广告点击超过100次的黑名单用户，并写入MySQL中
        // tuple: yyyyMMdd_userId_adId, clickCount
        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(tuple ->{
            String[] keySplitted = tuple._1.split("_");
            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplitted[0]));
            Long userId = Long.valueOf(keySplitted[1]);
            Long adId = Long.valueOf(keySplitted[2]);

            IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
            Long clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userId,adId);
            if(clickCount >100)
                return true;
            else
                return false;
        });
        // 5.2. distinct user_id
        JavaDStream<Long> blacklistUserIdDStream = blacklistDStream.map(tuple -> tuple._2);
        JavaDStream<Long> distinctBlacklistUserIdDStream = blacklistUserIdDStream.transform(
                rdd -> rdd.distinct());
        // 5.3. insert into table ad_blacklist
        distinctBlacklistUserIdDStream.foreachRDD(rdd -> rdd.foreachPartition(partitionOfRecords ->{
            List<AdBlacklist> adBlacklists = new ArrayList<>();
            while (partitionOfRecords.hasNext()){
                Long userId = Long.valueOf(partitionOfRecords.next());

                AdBlacklist adBlacklist = new AdBlacklist();
                adBlacklist.setUserId(userId);

                adBlacklists.add(adBlacklist);
            }
            IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
            adBlacklistDAO.insertBatch(adBlacklists);

        }));
    }

    private static JavaPairDStream<String, String> filterByBlackList(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        return adRealTimeLogDStream.transformToPair(rdd -> {
                IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                List<AdBlacklist> adBlacklists =  adBlacklistDAO.findAll();
                List<Tuple2<Long, Boolean >> tuples = new ArrayList<>();
                for(AdBlacklist adBlacklist: adBlacklists){
                    tuples.add(new Tuple2<>(adBlacklist.getUserId(), true));
                }
                // convert tuples to RDO <user_id, true>
                JavaSparkContext sc = new JavaSparkContext(rdd.context());
                JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);
                // map original rdd to <user_id, string>
                JavaPairRDD<Long, Tuple2<String, String>> rdd2 = rdd.mapToPair(tuple -> {
                    String log = tuple._2;
                    String[] logSplitted = log.split(",");
                    Long userId = Long.valueOf(logSplitted[3]);
                    return new Tuple2<>(userId,tuple);
                });
                // left join rdd2 and blacklistRDD
                JavaPairRDD<Long,Tuple2<Tuple2<String, String>, Optional<Boolean>>> rdd3 = rdd2.leftOuterJoin(blacklistRDD);
                // filter
                JavaPairRDD<Long,Tuple2<Tuple2<String, String>, Optional<Boolean>>> rdd4 = rdd3.filter(tuple ->{
                    Optional<Boolean> optional = tuple._2._2;
                    if(optional.isPresent() && optional.get()){
                        return false;
                    } else {
                        return true;
                    }
                });

                // map to that with original format <string,string>
                JavaPairRDD<String,String> resultRDD = rdd4.mapToPair(tuple ->{
                    return tuple._2._1;
                });

                return resultRDD;
            });
    }
}
