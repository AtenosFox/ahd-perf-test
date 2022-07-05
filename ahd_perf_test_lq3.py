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
    def long_query_3(self):
        self.client.execute_query(self.conn_string,
                                '''
                       SELECT count(*)
FROM (WITH le AS 
	(SELECT *
	FROM dl_spark.legal_entity_actual a
	WHERE a.inn = '7707049388'
			AND kpp ='784201001' ) , str AS 
		(SELECT *
		FROM dl_spark.state_contract_participant_actual a
		WHERE a.spark_id IN 
			(SELECT DISTINCT le.spark_id
			FROM le) )
			SELECT a.inn ,
		 a.ogrn ,
		 a.kpp ,
		 dcpa.participant_inn_code customer_inn ,
		 dcpa.participant_ogrn_code customer_ogrn ,
		 lea.kpp customer_kpp ,
		 lea.okopf_code customer_okopf_code ,
		 lea.full_name_rus full_name_rus ,
		 dcpa.participant_name short_name_rus ,
		 sca.contract_date ,
		 sca.currency_code ,
		 sca.contract_sum ,
		 sca.federallaw_code ,
		 sca.contract_subject_txt
			FROM le a
			JOIN str b
				ON a.spark_id = b.spark_id
					AND b.state_contract_role_code='2'--Поставщик
			JOIN dl_spark.dct_contract_participant_actual dcpa_supp
				ON a.inn=dcpa_supp.participant_inn_code
					AND b.contract_participant_code = dcpa_supp.code --join dl_spark.dct_state_contract_role_actual dscra --
				ON b.state_contract_role_code = dscra.code --
					AND dscra.code = ''1'' --Заказчик
			JOIN str b2
				ON b.contract_code=b2.contract_code
					AND b2.state_contract_role_code='1'--Заказчик
			JOIN dl_spark.dct_contract_participant_actual dcpa
				ON b2.contract_participant_code = dcpa.code
			LEFT JOIN dl_spark.legal_entity_actual lea
				ON dcpa.participant_spark_id = lea.spark_id
			JOIN dl_spark.state_contract_actual sca
				ON b.contract_code = sca.code) a; 
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
