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
    def long_query_8(self):
        self.client.execute_query(self.conn_string,
                                '''
                                  SELECT count(*)
FROM 
	(SELECT realty_bkh,
		 deal_bkh,
		 ods_bkh,
		 region_code,
		 cadastral_num,
		 cadastral_doc_num,
		 registration_date,
		 cancel_date,
		 deal_num,
		 deal_condition_txt,
		 subject_description,
		 ods_description,
		 deal_price_txt,
		 deal_price_amt,
		 room_number_txt,
		 room_name,
		 floor_number_txt,
		 plan_number_txt,
		 room_area_amt,
		 transfer_deadline_txt,
		 now()::date AS create_dt
	FROM 
		(SELECT d.bkh AS deal_bkh,
		 d4.bkh AS ods_bkh,
		 d.registration_date,
		 d.cancel_date,
		 deal_num,
		 d.region_code,
		 d3.bkh AS realty_bkh,
		 deal_condition_txt,
		 subject_description,
		 ods_description,
		 deal_price_txt,
		 cadastral_num,
		 cadastral_doc_num,
		 deal_price_amt,
		 room_number_txt,
		 room_name,
		 floor_number_txt,
		 plan_number_txt,
		 room_area_amt,
		 transfer_deadline_txt
		FROM dl_egrn_rosreestr.realty_actual d3
		JOIN dl_egrn_rosreestr.deal_actual d
			ON d3.bkh = d.realty_bkh
		JOIN 
			(SELECT d2.*
			FROM dl_egrn_rosreestr.cadastral_doc_actual d2
			JOIN dl_egrn_rosreestr.dct_cadastral_doc_type_actual dct
				ON dct.bkh = d2.cadastral_doc_type_bkh
			WHERE dct.cadastral_doc_type_code::bigint = 558401010201) d2
				ON d.bkh = d2.reference_bkh --
			LEFT JOIN dl_egrn_rosreestr.share_subject_description_actual d4
				ON d.bkh = d4.reference_bkh
			LEFT JOIN dl_egrn_rosreestr.realty_address_actual d5
				ON d3.bkh = d5.realty_bkh
			LEFT JOIN dl_egrn_rosreestr.dct_region_actual d6
				ON d6.bkh = d5.realty_region_bkh
			WHERE TRUE
					AND extract(year
			FROM d.registration_date) >= 2021) foo) a; 
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
