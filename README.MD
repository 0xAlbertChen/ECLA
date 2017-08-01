项目地址
https://github.com/xy83918/ECLA

开发IDE
https://www.jetbrains.com/idea/download/#section=windows

jdk 下载1.7
http://www.oracle.com/technetwork/java/javase/archive-139210.html


https://www.mysql.com/downloads/

MySQL windows版
http://sw.bos.baidu.com/sw-search-sp/software/d59738042c504/mysql-5.7.17.msi


sql脚本
```

CREATE DATABASE spark_project
USE spark_project
/*
SQLyog Ultimate v12.09 (64 bit)
MySQL - 5.7.17-log 
*********************************************************************
*/
/*!40101 SET NAMES utf8 */;

create table `task` (
	`task_id` int (11),
	`task_name` varchar (765),
	`create_time` varchar (765),
	`start_time` varchar (765),
	`finish_time` varchar (765),
	`task_type` varchar (765),
	`task_status` varchar (765),
	`task_param` text 
); 
insert into `task` (`task_id`, `task_name`, `create_time`, `start_time`, `finish_time`, `task_type`, `task_status`, `task_param`) values('1','测试任务',NULL,NULL,NULL,NULL,NULL,'{\"startAge\":[\"10\"],\"endAge\":[\"50\"],\"startDate\":[\"2017-07-26\"],\"endDate\":[\"2017-07-26\"],\"sex\":[\"male\"]}');

```


加载main方法 第 60多行

```
System.out.println (sessionid2actionRDD.count ());
        for (Tuple2<String,Row> tuple: sessionid2actionRDD.take ( 10 )){

            System.out.println (tuple._2 ());
        }
```
