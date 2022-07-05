from __future__ import absolute_import
from __future__ import print_function
from sqlalchemy import create_engine
from locust import User, between, TaskSet, task, events
import time
import configparser

config = configparser.ConfigParser()


def create_conn(conn_string):
    print("Connecting to ADB")
    return create_engine('postgresql+psycopg2://' + conn_string).connect()


def execute_query(conn_string, query):
    _conn = create_conn(conn_string)
    rs = _conn.execute(query)
    return rs


'''
  The ADB client that wraps the actual query
'''


class ADBClient:
    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                res = execute_query(*args, **kwargs)
                # print('Result ----------->' + str(res.fetchone()))
                events.request_success.fire(request_type="postgresql",
                                            name=name,
                                            response_time=int((time.time() - start_time) * 1000),
                                            response_length=res.rowcount)
            except Exception as e:
                events.request_failure.fire(request_type="postgresql",
                                            name=name,
                                            response_time=int((time.time() - start_time) * 1000),
                                            exception=e)

                print('error {}'.format(e))

        return wrapper


class CustomTaskSet(TaskSet):
    config.read('config.ini')
    conn_string = config['DEFAULT']['conn_string']


    @task(1)
    def long_query_1(self):
        self.client.execute_query(self.conn_string,
                                '''
                                 SELECT count(*)
FROM 
	(SELECT Ar.Bk_Hash,
		 Ar.Spark_Id,
		 Ar.Balance_Type,
		 dap.period_name,
		 Ar.Form_Name,
		 Ar.Financial_Indicator_Code,
		 TRUE AS status,
		 Ar.Section_Name,
		 Ar.Line_Code,
		 Ar.Line_Name,
		 Ar.Line_Value
	FROM Dl_Spark.Accounting_Report_actual ar
	INNER JOIN 
		(SELECT MAX(ar.Accounting_Report_id) m_id
		FROM Dl_Spark.Accounting_Report ar
		INNER JOIN Snbx_Creditroad.Cd_Inn Ci
			ON ci.spark_id = ar.spark_id
		WHERE ar.Valid_From
			BETWEEN to_date('2021-04-01', 'YYYY-MM-DD') --:sd --1922-06-06 00:00:00.000000 -- to_date('1922-06-06', 'YYYY-MM-DD')
				AND to_date('2021-04-18', 'YYYY-MM-DD') ---:ed -- 2022-06-09 00:00:00.000000 -- to_date('2022-06-09', 'YYYY-MM-DD')
		GROUP BY  ar.Bk_Hash) arm
			ON arm.m_id = ar.Accounting_Report_id
		INNER JOIN dl_spark.dct_accounting_period dap
			ON dap.code = Ar.accounting_period_code
		WHERE line_code = '2100') a;   
                                '''
                                  )


class ADBUser(User):
    min_wait = 0
    max_wait = 0
    tasks = [CustomTaskSet]
    wait_time = between(min_wait, max_wait)

    def __init__(self, environment):
        super().__init__(environment)
        self.client = ADBClient()
