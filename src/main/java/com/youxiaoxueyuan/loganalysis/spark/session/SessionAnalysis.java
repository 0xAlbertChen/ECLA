package com.youxiaoxueyuan.loganalysis.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.youxiaoxueyuan.loganalysis.conf.ConfigurationManager;
import com.youxiaoxueyuan.loganalysis.constant.Constants;
import com.youxiaoxueyuan.loganalysis.dao.ITaskDAO;
import com.youxiaoxueyuan.loganalysis.dao.factory.DAOFactory;
import com.youxiaoxueyuan.loganalysis.domain.Task;
import com.youxiaoxueyuan.loganalysis.test.MockData;
import com.youxiaoxueyuan.loganalysis.util.DateUtils;
import com.youxiaoxueyuan.loganalysis.util.ParamUtils;
import com.youxiaoxueyuan.loganalysis.util.StringUtils;
import com.youxiaoxueyuan.loganalysis.util.ValidUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * Created by Albert on 2017/7/26.
 */
public class SessionAnalysis {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf ().setAppName ( Constants.SPARK_APP_NAME_SESSION_ANALYSIS ).setMaster ( "local" );

        JavaSparkContext sc = new JavaSparkContext ( conf );

        SQLContext sqlContext = getSQLContext ( sc.sc () );

        mockData ( sc, sqlContext );

        ITaskDAO taskDAO = DAOFactory.getTaskDAO ();

        long taskid = ParamUtils.getTaskIdFromArgs ( args, Constants.SPARK_LOCAL_TASKID_SESSION );
        Task task = taskDAO.findById ( taskid );
        if (task == null) {
            System.out.println ( new Date () + ": cannot find this task with id [" + taskid + "]." );
            return;
        }

        JSONObject taskParam = JSONObject.parseObject ( task.getTaskParam () );

        JavaRDD <Row> actionRDD = getActionRDDByDateRange ( sqlContext, taskParam );
        JavaPairRDD <String, Row> sessionid2actionRDD = getSessionid2ActionRDD ( actionRDD );

        JavaPairRDD<String,String> sessionid2AggrInfoRDD = aggregateBySession ( sc,sqlContext,sessionid2actionRDD );


        System.out.println (sessionid2AggrInfoRDD.count ());
        for (Tuple2<String,String> tuple: sessionid2AggrInfoRDD.take ( 10 )){

            System.out.println (tuple._2 ());
        }


        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2AggrInfoRDD, taskParam);

        System.out.println (filteredSessionid2AggrInfoRDD.count ());
        for (Tuple2<String,String> tuple: filteredSessionid2AggrInfoRDD.take ( 10 )){

            System.out.println (tuple._2 ());
        }

        sc.close ();


    }

    private static SQLContext getSQLContext(SparkContext sc) {
        boolean runModel = ConfigurationManager.getBoolean ( Constants.SPARK_RUN_MODEL );

        if (runModel) {
            return new SQLContext ( sc );
        } else {
            return new HiveContext ( sc );

        }

    }

    public static JavaPairRDD <String, Row> getSessionid2ActionRDD(JavaRDD <Row> actionRDD) {
        return actionRDD.mapToPair ( new PairFunction <Row, String, Row> () {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2 <String, Row> call(Row row) throws Exception {
                return new Tuple2 <String, Row> ( row.getString ( 2 ), row );
            }

        } );

    }

    public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean runModel = ConfigurationManager.getBoolean ( Constants.SPARK_RUN_MODEL );
        if (runModel) {
            MockData.mock ( sc, sqlContext );
        }
    }

    public static JavaRDD <Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam ( taskParam, Constants.PARAM_START_DATE );
        String endDate = ParamUtils.getParam ( taskParam, Constants.PARAM_END_DATE );

        String sql = "select * " + "from user_visit_action " + "where date>='" + startDate + "' " + "and date<='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql ( sql );

        return actionDF.javaRDD ();
    }

    private static JavaPairRDD <String, String> aggregateBySession(JavaSparkContext sc, SQLContext sqlContext, JavaPairRDD <String, Row> sessinoid2actionRDD) {

        JavaPairRDD <String, Iterable <Row>> sessionid2ActionsRDD = sessinoid2actionRDD.groupByKey ();

        JavaPairRDD <Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair (

                new PairFunction <Tuple2 <String, Iterable <Row>>, Long, String> () {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2 <Long, String> call(Tuple2 <String, Iterable <Row>> tuple) throws Exception {
                        String sessionid = tuple._1;
                        Iterator <Row> iterator = tuple._2.iterator ();

                        StringBuffer searchKeywordsBuffer = new StringBuffer ( "" );
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer ( "" );

                        Long userid = null;

                        Date startTime = null;
                        Date endTime = null;
                        int stepLength = 0;

                        while (iterator.hasNext ()) {
                            Row row = iterator.next ();
                            if (userid == null) {
                                userid = row.getLong ( 1 );
                            }
                            String searchKeyword = row.getString ( 5 );
                            Long clickCategoryId = row.getLong ( 6 );


                            if (StringUtils.isNotEmpty ( searchKeyword )) {
                                if (!searchKeywordsBuffer.toString ().contains ( searchKeyword )) {
                                    searchKeywordsBuffer.append ( searchKeyword + "," );
                                }
                            }
                            if (clickCategoryId != null) {
                                if (!clickCategoryIdsBuffer.toString ().contains ( String.valueOf ( clickCategoryId ) )) {
                                    clickCategoryIdsBuffer.append ( clickCategoryId + "," );
                                }
                            }

                            Date actionTime = DateUtils.parseTime ( row.getString ( 4 ) );

                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }

                            if (actionTime.before ( startTime )) {
                                startTime = actionTime;
                            }
                            if (actionTime.after ( endTime )) {
                                endTime = actionTime;
                            }

                            stepLength++;
                        }

                        String searchKeywords = StringUtils.trimComma ( searchKeywordsBuffer.toString () );
                        String clickCategoryIds = StringUtils.trimComma ( clickCategoryIdsBuffer.toString () );

                        long visitLength = (endTime.getTime () - startTime.getTime ()) / 1000;

                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime ( startTime );

                        return new Tuple2 <Long, String> ( userid, partAggrInfo );
                    }

                } );

        String sql = "select * from user_info";
        JavaRDD <Row> userInfoRDD = sqlContext.sql ( sql ).javaRDD ();

        JavaPairRDD <Long, Row> userid2InfoRDD = userInfoRDD.mapToPair (

                new PairFunction <Row, Long, Row> () {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2 <Long, Row> call(Row row) throws Exception {
                        return new Tuple2 <Long, Row> ( row.getLong ( 0 ), row );
                    }

                } );


        JavaPairRDD <Long, Tuple2 <String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join ( userid2InfoRDD );

        JavaPairRDD <String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair (

                new PairFunction <Tuple2 <Long, Tuple2 <String, Row>>, String, String> () {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2 <String, String> call(Tuple2 <Long, Tuple2 <String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionid = StringUtils.getFieldFromConcatString ( partAggrInfo, "\\|", Constants.FIELD_SESSION_ID );

                        int age = userInfoRow.getInt ( 3 );
                        String professional = userInfoRow.getString ( 4 );
                        String city = userInfoRow.getString ( 5 );
                        String sex = userInfoRow.getString ( 6 );

                        String fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" + Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "=" + sex;

                        return new Tuple2 <String, String> ( sessionid, fullAggrInfo );
                    }

                } );


        return sessionid2FullAggrInfoRDD;
    }


    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam) {
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                new Function<Tuple2<String,String>, Boolean> () {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        String aggrInfo = tuple._2;

                        if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        return true;
                    }



                });

        return filteredSessionid2AggrInfoRDD;
    }

}
