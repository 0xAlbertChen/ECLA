package com.youxiaoxueyuan.loganalysis.constant;

public interface Constants {

	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	String JDBC_URL_PROD = "jdbc.url.prod";
	String JDBC_USER_PROD = "jdbc.user.prod";
	String JDBC_PASSWORD_PROD = "jdbc.password.prod";
	String SPARK_RUN_MODEL = "spark.runModel";
	String SPARK_LOCAL_TASKID_SESSION = "spark.local.taskid.session";

	String SPARK_APP_NAME_SESSION_ANALYSIS = "SessionAnalysis";
	String FIELD_SESSION_ID = "sessionid";
	String FIELD_SEARCH_KEYWORDS = "searchKeywords";
	String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
	String FIELD_VISIT_LENGTH = "visitLength";
	String FIELD_STEP_LENGTH = "stepLength";
	String FIELD_START_TIME = "startTime";
	String FIELD_AGE = "age";
	String FIELD_PROFESSIONAL = "professional";
	String FIELD_CITY = "city";
	String FIELD_SEX = "sex";

	String PARAM_START_DATE = "startDate";
	String PARAM_END_DATE = "endDate";
	String PARAM_START_AGE = "startAge";
	String PARAM_END_AGE = "endAge";
	String PARAM_PROFESSIONALS = "professionals";
	String PARAM_CITIES = "cities";
	String PARAM_SEX = "sex";
	String PARAM_KEYWORDS = "keywords";
	String PARAM_CATEGORY_IDS = "categoryIds";


}
