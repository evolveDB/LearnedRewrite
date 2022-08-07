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
# from dbutils.pooled_db import PooledDB
import pandas as pd
import platform

if platform.system().lower() == 'windows':
    from DBUtils.PooledDB import PooledDB
else:
    from dbutils.pooled_db import PooledDB

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


def get_pooled_database(host, user, password, database, port=3306, max_connections=20):
    pool = PooledDB(
        pymysql,
        max_connections,
        host=host,
        user=user,
        port=port,
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


def get_table_columns(conn, table):
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


def get_table_rows(conn, table):
    # use desc
    cursor = conn.cursor()
    # Execute DESCRIBE statement
    cursor.execute("select count(*) from " + table)

    # Fetch and print the meta-data of the table
    fetched_result = cursor.fetchall()

    return fetched_result[0][0]


def generate_sql_rewrite_schema(conn):
    db_schema_list = []
    table_list = get_db_tables(conn)
    for table_name in table_list:
        table_column_list = get_table_columns(conn, table_name)
        table_rows = get_table_rows(conn, table_name)
        db_schema_list.append({'table': table_name, 'rows': table_rows, 'columns': table_column_list})
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


def query_performance_benchmark(pooled_db, sql_query_input, threads=1):
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
    # scheduler = BackgroundScheduler({'apscheduler.executors.default': {
    #     'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
    #     'max_workers': '1'}})

    # job = scheduler.add_job(monitor_thread_function, 'interval', seconds=10, args=(pooled_db,), name='mysql_monitor',
    #                         max_instances=1)
    # scheduler.start()
    total_query_cost = 0.0
    benchmark_result_list = []
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
            sql, query_cost = future.result()
            benchmark_result_list.append((sql, query_cost))
            total_query_cost += query_cost
            # sql, time_used = res.get()
        # result_output.write("total:%d, total time used:%.3f" % (len(async_future_list), total_query_cost))
        logger.info("input file:%s, total:%d, total query_latency:%.3f" % (
            sql_query_input.name, len(async_future_list), total_query_cost))
    # scheduler.shutdown(wait=False)
    return benchmark_result_list


def rewrite_performance_benchmark(pooled_db, origin_sql_query_input_file, result_output_file, threads, args):
    start_time = time.time()
    total_origin_query_cost = 0.0
    total_rewrite_query_cost = 0.0
    origin_input_ext = os.path.splitext(os.path.abspath(origin_sql_query_input_file))
    rewrite_output_file = origin_input_ext[0] + "_rewrite" + origin_input_ext[1]
    rewrite(pooled_db, origin_sql_query_input_file, rewrite_output_file, args)
    with open(origin_sql_query_input_file) as origin_query_input, open(
            rewrite_output_file) as rewrite_query_input, pd.ExcelWriter(result_output_file,
                                                                        engine="openpyxl") as excel_writer:


        origin_query_benchmark_list = query_performance_benchmark(pooled_db, origin_query_input, threads)
        rewrite_query_benchmark_list = query_performance_benchmark(pooled_db, rewrite_query_input, threads)

        # os.remove(temp_rewrite_output.name)
        columns = ['origin latency', 'optimized latency', 'improvement', 'origin query', 'rewrite query']
        data_rows = []
        for i in range(len(origin_query_benchmark_list)):
            origin_latency = origin_query_benchmark_list[i]
            optimized_latency = rewrite_query_benchmark_list[i]
            total_origin_query_cost += origin_latency[1]
            total_rewrite_query_cost += optimized_latency[1]
            improvement = 1 - optimized_latency[1] / origin_latency[1]
            data_rows.append([origin_latency[1], optimized_latency[1], "%.2f%%" % (100.0 * improvement), origin_latency[0],
                              optimized_latency[0]])

        output_dataframe = pd.DataFrame(data_rows, columns=columns)
        output_dataframe.to_excel(excel_writer, index=False)
        end_time = time.time()
        logger.info(
            "evaluation time used:%s, origin query cost:%.2f,rewrite query cost:%.2f" % (
                str(timedelta(seconds=end_time - start_time)), total_origin_query_cost, total_rewrite_query_cost))
    return total_origin_query_cost, total_rewrite_query_cost


def error_callback(value):
    logger.exception(value)


def multi_thread_query_function(pooled_db, sql, idx, total):
    try:
        conn = pooled_db.connection()
        cursor = conn.cursor()

        execute_time = 1

        query_cost = 0
        for i in range(execute_time):

            '''
            start_time = time.time()

            # cursor.execute("explain format=json %s" % sql)
            cursor.execute("%s" % sql)

            # fetched_row = cursor.fetchone()
            # explain_json = json.loads(fetched_row[0])
            # query_cost = float(explain_json['query_block']['cost_info']['query_cost'])

            end_time = time.time()
            # time_used = end_time - start_time
            query_cost = query_cost + end_time - start_time            
            '''

            cursor.execute("explain format=json %s" % sql)

            fetched_row = cursor.fetchone()
            explain_json = json.loads(fetched_row[0])
            query_cost = query_cost + float(explain_json['query_block']['cost_info']['query_cost'])

        query_cost = query_cost / execute_time

        logger.debug("idx:%d, total:%d, query latency:%.2f, sql:%s" % (idx, total, query_cost, sql))
        cursor.close()
        conn.close()
        return sql, query_cost
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


def rewrite(pooled_db, input_sql_file, output_sql_file, args):
    conn = pooled_db.connection()
    db_schema_list = generate_sql_rewrite_schema(conn)
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_schema_output:
        # temp_schema_output.writelines([json.dumps(db_schema_list, ensure_ascii=True, indent=True)])
        temp_schema_output.write(json.dumps(db_schema_list, ensure_ascii=True, indent=True))
        temp_schema_output.close()

        input_sql_abs_path = os.path.abspath(input_sql_file)
        output_sql_abs_path = os.path.abspath(output_sql_file)
        if os.path.exists("target"):
            command = 'cd target && java -jar ai4db.jar %s %s %s %s %s %s %s %s' % (
                temp_schema_output.name, args.database, args.host, args.port, args.user, args.password,
                input_sql_abs_path, output_sql_abs_path)
        else:
            command = 'java -jar ai4db.jar %s %s %s %s %s %s %s %s %s %s %s' % (
                temp_schema_output.name, args.database, args.host, args.port, args.user, args.password,
                input_sql_abs_path, output_sql_abs_path)
        logger.debug("rewrite command:%s" % command)
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
    parser.add_argument('-u', '--user', dest='user', default='root',
                        help='set mysql user')
    parser.add_argument('-p', '--password', dest='password', default='root',
                        help='set mysql password')
    parser.add_argument('-D', '--database', dest='database',
                        help='set mysql database')

    parser.add_argument('-t', '--thread', dest='thread', type=int, default=os.cpu_count() * 2,
                        help='set concurrent threads')
    parser.add_argument('-a', '--action', dest='action',
                        help='set action, current available actions: rewrite/benchmark')

    parser.add_argument("input_file", metavar="input_file", help="set input file")
    parser.add_argument('output_file', metavar='output_file', help='set output file')
    # parser.print_help(sys.stdout)
    args = parser.parse_args()

    logging.getLogger().setLevel(logging.INFO)

    if args.action == 'rewrite':
        pooled_db = get_pooled_database(args.host, args.user, args.password, args.database, args.port)
        rewrite(pooled_db, args.input_file, args.output_file, args)
    elif args.action == "benchmark":
        pooled_db = get_pooled_database(args.host, args.user, args.password, args.database, args.port)
        rewrite_performance_benchmark(pooled_db, args.input_file, args.output_file, args.thread, args)
        # query_performance_benchmark(pooled_db, args.input_file, args.output_file, args.thread)
    else:
        parser.print_help(sys.stderr)
