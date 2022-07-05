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
    def long_query_7(self):
        self.client.execute_query(self.conn_string,
                                '''
                                SELECT count(*) FROM(
WITH realty_location AS 
	(SELECT aa.src_realty_id,
		 aa.region_name
	FROM dl_cian.address_actual aa) 
		(SELECT DISTINCT
			ON (a.src_cian_id) a.src_cian_id, loc.region_name, a.price_sqm_amt, a.price_amt, a.currency_name, a.realty_latitude, a.realty_longitude, a.room_cnt, a.total_area_amt, daca.advert_category_code, daca.advert_category_name, dbca.building_class_type, dbca.building_class_name
		FROM dl_cian.advert a
		LEFT JOIN dl_cian.dct_advert_category_actual daca
			ON a.advert_category_bkh = daca.bkh
		LEFT JOIN dl_cian.dct_status_actual dsa
			ON a.status_bkh = dsa.bkh
		LEFT JOIN realty_location loc
			ON loc.src_realty_id = a.src_realty_id
		LEFT JOIN dl_cian.dct_housing_complex_block_actual dhcba
			ON a.housing_complex_block_code = dhcba.housing_complex_block_code
		LEFT JOIN dl_cian.dct_building_class_actual dbca
			ON dhcba.building_class_bkh = dbca.bkh
		WHERE a.deleted_flag = FALSE
				AND loc.region_name = 'Москва'
				AND dsa.status_code = 1
				AND a.valid_from < '2022-06-01'
		ORDER BY  a.src_cian_id, a.valid_from DESC nulls LAST)
		UNION
		ALL 
			(SELECT DISTINCT
				ON (a.src_cian_id) a.src_cian_id, loc.region_name, a.price_sqm_amt, a.price_amt, a.currency_name, a.realty_latitude, a.realty_longitude, a.room_cnt, a.total_area_amt, daca.advert_category_code, daca.advert_category_name, dbca.building_class_type, dbca.building_class_name
			FROM dl_cian.advert a
			LEFT JOIN dl_cian.dct_advert_category_actual daca
				ON a.advert_category_bkh = daca.bkh
			LEFT JOIN dl_cian.dct_status_actual dsa
				ON a.status_bkh = dsa.bkh
			LEFT JOIN realty_location loc
				ON loc.src_realty_id = a.src_realty_id
			LEFT JOIN dl_cian.dct_housing_complex_block_actual dhcba
				ON a.housing_complex_block_code = dhcba.housing_complex_block_code
			LEFT JOIN dl_cian.dct_building_class_actual dbca
				ON dhcba.building_class_bkh = dbca.bkh
			WHERE a.deleted_flag = FALSE
					AND a.valid_from >= '2022-06-01'
					AND a.valid_from <= '2022-06-30 23:59:59'
					AND dsa.status_code = 1
					AND loc.region_name = 'Москва'
			ORDER BY  a.src_cian_id, a.valid_from DESC nulls LAST)) a; 
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
