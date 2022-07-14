
# How to do sql rewrite and performance test


## Dependency
* Java runtime environment
* Python: pymysql、DBUtils、apscheduler、pandas, install them use pip or conda
* MySQL: turn on at least slow log

## Procedure
1. use ```python bin/ai4db_evaluation.py --help``` to get script usage help
2. prepare origin.sql: get all your daily query list(if difficult, get slow query list) and save them to file, e.g. origin.sql, you can take data/sample/origin.sql as an example and find guide to generate valid input from FAQ
3. benchmark origin.sql: 
   - ```python bin/ai4db_evaluation.py -uroot -proot -Dtest_db -a benchmark data/sample/origin.sql data/sample/benchmark_out.xlsx```
   - open data/sample/benchmark_out.xlsx to get the benchmark result

## FAQ
Q. input/output example?

You can find them at data/sample directory

Q. how to get query log?

you can turn on **all query log** for mysql use method in this article:https://blog.csdn.net/u010735147/article/details/81871560

generally for mysql, execute following commands:
* ```SET GLOBAL general_log = 'ON'; ```
* ```show variables like 'general_log_file'; ```
* then you can find all query in the file ${general_log_file}
* use ```cat general.log|grep "SELECT"|awk -F'\t' 'NR>=4{print $3";"}'``` to generate valid input file for evaluation

if difficult to turn on all query log for your production environment, you can turn on **slow log** use method in this article:https://www.cnblogs.com/luyucheng/p/6265594.html

generally for mysql, execute following commands:
* ```set global slow_query_log=ON;```
* ```show variables like 'slow_query_log_file';```
* then you can find all query in the file ${slow_query_log_file}
* use ```mysqldumpslow -a  slow.log|grep "SELECT"|awk '{sub(/^[ ]/,"");sub(/[ ]$/,"");print $0";";}'``` to get valid input file for evaluation

if slow_query_log_file has not been set, use command ```set global slow_query_log_file='/xxx/xxx/data/slow.log';``` to set slow_query_log_file 


Q. how to check mysql slow log configuration?
```commandline
mysql> show variables like 'slow_query%';
mysql> show variables like 'long_query_time';
mysql> SHOW VARIABLES LIKE 'log_output';
mysql> SHOW CREATE TABLE mysql.slow_log;
mysql> SET GLOBAL log_output='TABLE';#dangerous, only for test, not for production
```

## Misc
```commandline
python bin/ai4db_evaluation.py --host 127.0.0.1 -u root -p root -D imdbload -a rewrite data/job/origin.sql data/job/rewrite.sql
python bin/ai4db_evaluation.py -uroot -proot -Dimdbload -a benchmark data/sample/origin.sql data/sample/benchmark_out.xlsx
```
