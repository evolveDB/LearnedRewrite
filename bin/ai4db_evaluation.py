#!/usr/bin/python
# -*- coding: UTF-8 -*-
# reload(sys)
# sys.setdefaultencoding('utf-8')
import logging
import os
import sys
import json
import pymysql
import os
import re
# import multiprocessing
import time
from datetime import timedelta
import argparse
import tempfile
from concurrent.futures.thread import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from DBUtils.PooledDB import PooledDB

# from DBUtils.PooledDB import PooledDB

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s\t%(name)s\t%(levelname)s\t%(lineno)d\t%(message)s')

# StreamHandler
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(level=logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# FileHandler
file_handler = logging.FileHandler('sf.log')
file_handler.setLevel(level=logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s\t%(name)s\t%(levelname)s\t%(lineno)d\t%(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def get_pooled_database(host, user, password, database, max_connections=20, port=3306):
    pool = PooledDB(
        pymysql,
        max_connections,
        host=host,
        user=user,
        port=3306,
        passwd=password,
        db=database,
        use_unicode=True)
    return pool


def get_db_conn(host, user, password, db, port=3306):
    conn = pymysql.connect(host=host, user=user, passwd=password, db=db, port=port, charset='utf8')
    return conn


def get_db_tables(conn):
    # use desc
    cursor = conn.cursor()
    # Execute DESCRIBE statement
    cursor.execute("show tables")

    # Fetch and print the meta-data of the table
    fetched_result = cursor.fetchall()
    table_list = [item[0] for item in fetched_result]
    return table_list


def get_table_schema(conn, table):
    # use desc
    cursor = conn.cursor()
    # Execute DESCRIBE statement
    cursor.execute("DESCRIBE %s" % table)

    # Fetch and print the meta-data of the table
    fetched_result = cursor.fetchall()
    schema_list = []
    for item in fetched_result:
        idx = item[1].find('(')
        schema_list.append({'name': item[0], 'type': item[1][0:idx] if idx > 0 else item[1]})
    return schema_list


def generate_sql_rewrite_schema(conn):
    db_schema_list = []
    table_list = get_db_tables(conn)
    for table_name in table_list:
        table_schema_list = get_table_schema(conn, table_name)
        db_schema_list.append({'table': table_name, 'rows': 1000, 'columns': table_schema_list})
    return db_schema_list


'''SELECT TABLE_SCHEMA AS `Database`,
TABLE_NAME AS `Table`,
ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 ) AS `Size (kB)`
FROM information_schema.TABLES where TABLE_SCHEMA='imdbload'
ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;
目前缺失的表：
movie_link
movie_info
person_info
info_type
name
role_type
keyword
info_type'''


def preprocess_job_query(job_query_dir, output_file_name):
    with open(output_file_name, "w") as output:
        for (dir_path, dir_names, file_names) in os.walk(job_query_dir):
            for file_name in file_names:
                root, ext = os.path.splitext(file_name)
                if ext != '.sql':
                    continue
                if not re.match(r'\d{1,2}[a-z]', root):
                    continue
                with open(os.path.join(job_query_dir, file_name)) as input:
                    lines = input.read().strip()
                    # one_line = lines.replace('\s', ' ')
                    one_line = re.sub(r'\s+', ' ', lines).replace(" character,", " cname,")
                    output.write("%s\n" % one_line)


def query_performance_benchmark(pooled_db, sql_query_input_file, result_output_file, threads=1):
    start_time = time.time()
    total_time_used = 0.0
    with open(sql_query_input_file) as sql_query_input, open(result_output_file, "w") as result_output:
        line = sql_query_input.readline()
        sql_list = []
        while line:
            line = line.strip()
            if line:
                sql_list.append(line)
            line = sql_query_input.readline()
        # with ThreadPoolExecutor(max_workers=1) as monitor:
        #     monitor_future = monitor.submit(monitor_thread_function, conn, 10)
        #     logger.debug("monitor future:%s" % monitor_future)
        # monitor.shutdown()
        scheduler = BackgroundScheduler({'apscheduler.executors.default': {
            'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
            'max_workers': '1'}})

        job = scheduler.add_job(monitor_thread_function, 'interval', seconds=5, args=(pooled_db,), name='mysql_monitor',
                                max_instances=1)
        scheduler.start()

        with ThreadPoolExecutor(max_workers=threads) as executor:
            async_future_list = []
            idx = 1
            for sql in sql_list:
                try:
                    future = executor.submit(multi_thread_query_function,
                                             pooled_db, sql, idx, len(sql_list))
                    async_future_list.append(future)
                    idx += 1
                except Exception as e:
                    logger.exception(str(e))
            executor.shutdown()
            for future in async_future_list:
                sql, time_used = future.result()
                # sql, time_used = res.get()
                result_output.write("%.3f:%s\n" % (time_used, sql))
                total_time_used += time_used
            result_output.write("total:%d, total time used:%.3f" % (len(async_future_list), total_time_used))
            logger.info("total:%d, total time used:%.3f" % (len(async_future_list), total_time_used))
        scheduler.shutdown(wait=False)
    end_time = time.time()
    logger.info(
        "total query time used include read/write file:%s, total query time used without read/write file:%s, for sql file:%s" % (
            str(timedelta(seconds=end_time - start_time)), str(timedelta(seconds=total_time_used)),
            sql_query_input_file))
    return total_time_used


def error_callback(value):
    logger.exception(value)


def multi_thread_query_function(pooled_db, sql, idx, total):
    try:
        start_time = time.time()
        conn = pooled_db.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        fetched_row = cursor.fetchone()
        end_time = time.time()
        time_used = end_time - start_time
        logger.debug("idx:%d, total:%d, time used:%.2f seconds, first row:%s" % (idx, total, time_used, fetched_row))
        cursor.close()
        conn.close()
        return sql, time_used
    except Exception as e:
        logger.exception("idx:%s, sql:%s" % (idx, sql))
        return sql, -1


def monitor_thread_function(pooled_db):
    # show global status like 'Question%'; QPS
    # show global status like ‘Com_commit’
    # show global status like ‘Com_rollback’
    # TPS =（Com_commit + Com_rollback）/seconds
    # show global status like ‘Max_used_connections’
    # show global status like ‘Threads%’
    # show variables like ‘max_connections’
    conn = pooled_db.connection()
    performance_metric = {}
    cursor = conn.cursor()
    cursor.execute("show status like 'Question%';")
    qps_row = cursor.fetchone()
    performance_metric[qps_row[0]] = qps_row[1]
    cursor.execute("show global status like 'Com_commit';")
    commit_row = cursor.fetchone()
    performance_metric[commit_row[0]] = commit_row[1]
    cursor.execute("show global status like 'Com_rollback';")
    rollback_row = cursor.fetchone()
    performance_metric[rollback_row[0]] = rollback_row[1]
    cursor.execute("show global status like 'Threads_connected';")
    threads_row = cursor.fetchone()
    performance_metric[threads_row[0]] = threads_row[1]
    logger.info("################# performance metric #################")
    performance_keys = list(performance_metric.keys())
    performance_values = [performance_metric[key] for key in performance_keys]
    logger.info("{:<8} {:<15} {:<10} {:<10}".format(performance_keys[0], performance_keys[1], performance_keys[2],
                                                    performance_keys[3]))
    logger.info(
        "{:<8} {:<15} {:<10} {:<10}".format(performance_values[0], performance_values[1], performance_values[2],
                                            performance_values[3]))
    cursor.close()
    conn.close()


def rewrite(conn, input_sql_file, output_sql_file):
    db_schema_list = generate_sql_rewrite_schema(conn)
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_schema_output:
        # temp_schema_output.writelines([json.dumps(db_schema_list, ensure_ascii=True, indent=True)])
        temp_schema_output.write(json.dumps(db_schema_list, ensure_ascii=True, indent=True))
        temp_schema_output.close()
        command = 'java -jar ai4db.jar %s %s %s' % (temp_schema_output.name, input_sql_file, output_sql_file)
        os.system(command)
        os.remove(temp_schema_output.name)
    conn.close()


def setup_job_database(conn, schema_sql, index_sql, csv_files_dir):
    with open(schema_sql) as schema_sql_input, open(index_sql) as index_sql_input:
        schema_sql_text = schema_sql_input.read()
        cursor = conn.cursor()
        for sql in schema_sql_text.split(";"):
            sql = sql.strip()
            if not sql:
                continue
            # cursor.execute(sql)
        print("done create tables")
        index_sql_text = index_sql_input.read()
        for sql in index_sql_text.split(";"):
            sql = sql.strip()
            if not sql:
                continue
            # cursor.execute(sql)
        print("done create indexes")
    for (dir_path, dir_names, file_names) in os.walk(csv_files_dir):
        for file_name in file_names:
            root, ext = os.path.splitext(file_name)
            if ext != '.csv':
                continue
            sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',';" % (dir_path + "/" + file_name,
                                                                                           root)
            load_sql_file = os.path.join(dir_path, root + ".sql")
            open(load_sql_file, "w").write(sql)
            print(sql)
            command = 'mysql -h 127.0.0.1 -P 3306 -u root -p root -D imdbload < %s' % load_sql_file
            os.system(command)
            os.remove(load_sql_file)
            # cursor.execute(sql)
            # conn.commit()


# conn = get_db_conn('127.0.0.1', 'root', 'root', 'imdbload')
# db_schema_list = generate_sql_rewrite_schema(conn)
# print(json.dumps(db_schema_list))
# print(os.getcwd())
# with open("../data/sample/sample_schema.json", "w") as imdb_schema_output:
#    imdb_schema_output.write(json.dumps(db_schema_list, ensure_ascii=True, indent=True))

# preprocess_job_query("C:/Users/coderush/code/join-order-benchmark", '../data/job/origin.sql')
# setup_job_database(conn, "../data/job/schema.sql", "../data/job/fkindexes.sql",
#                    "C:/Users/coderush/code/3-SQL-Rewriter/data/job-small/job/data")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='evaluation ai4db performance')
    parser.add_argument('--host', dest='host', default='127.0.0.1',
                        help='set mysql host')
    parser.add_argument('--port', dest='port', default=3306,
                        help='set mysql port')
    parser.add_argument('-u', '--user', dest='user',
                        help='set mysql user')
    parser.add_argument('-p', '--password', dest='password',
                        help='set mysql password')
    parser.add_argument('-D', '--database', dest='database',
                        help='set mysql database')

    parser.add_argument('-t', '--thread', dest='thread', type=int, default=1,
                        help='set concurrent threads')
    parser.add_argument('-a', '--action', dest='action',
                        help='set action, current available actions: rewrite/benchmark')

    parser.add_argument("input_file", metavar="input_file", help="set input file")
    parser.add_argument('output_file', metavar='output_file', help='set output file')
    # parser.print_help(sys.stdout)
    args = parser.parse_args()
    if args.action == 'rewrite':
        db_conn = get_db_conn(args.host, args.user, args.password, args.database, args.port)
        rewrite(db_conn, args.input_file, args.output_file)
    elif args.action == "benchmark":
        pooled_db = get_pooled_database(args.host, args.user, args.password, args.database)
        query_performance_benchmark(pooled_db, args.input_file, args.output_file, args.thread)
    else:
        parser.print_help(sys.stderr)
