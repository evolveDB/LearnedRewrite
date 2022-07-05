
# How to do sql rewrite and performance test



## Dependency
* Java runtime environment
* Python: pymysql、DBUtils、apscheduler, install them use pip or conda
* MySQL: turn on at least slow log

## Procedure
1. prepare my_origin.sql: get all your daily query list(if difficult, get slow query list) and save them to file, e.g. my_origin.sql, you can take data/sample/sample_test.sql as an example
2. rewrite all your query list with command: ```python bin/ai4db_evaluation.py --host xxx -u xxx -p xxx -D xxx -a rewrite my_origin.sql my_rewrite.sql```, e.g. ```python bin/ai4db_evaluation.py --host 127.0.0.1 -u root -p root -D test_db -a rewrite my_origin.sql my_rewrite.sql```
3. benchmark my_origin.sql and my_rewrite.sql: 
   - ```python bin/ai4db_evaluation.py -uroot -proot -Dtest_db -a benchmark my_origin.sql my_origin_out.txt```
   - ```python bin/ai4db_evaluation.py -uroot -proot -Dtest_db -a benchmark my_rewrite.sql my_rewrite_out.txt```
   - compare my_origin_out.txt and my_rewrite_out.txt to get the benchmark result

## FAQ
Q. input/output example?

You can find them at data/sample directory

Q. how to get query log?

You can google/bing/baidu to get the original query log and process it to the input format required by the script bin/ai4db_evaluation.py


## Misc
```commandline
python bin/ai4db_evaluation.py --host 127.0.0.1 -u root -p root -D imdbload -a rewrite data/job/origin.sql data/job/rewrite.sql
python bin/ai4db_evaluation.py -uroot -proot -Dimdbload -t 10 -a benchmark data/job/origin.sql data/sample/origin_out.txt
python bin/ai4db_evaluation.py -uroot -proot -Dimdbload -t 10 -a benchmark data/job/rewrite.sql data/sample/write_out.txt
```
