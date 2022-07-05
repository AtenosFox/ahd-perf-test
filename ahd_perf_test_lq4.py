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
    def long_query_4(self):
        self.client.execute_query(self.conn_string,
                                '''
                              SELECT count(*)
FROM 
	(SELECT "Custom SQL Query"."agg_ods_description" AS "agg_ods_description",
		 CAST("Custom SQL Query"."cadastral_doc_addition_txt" AS text) AS "cadastral_doc_addition_txt",
		 CAST("Custom SQL Query"."cadastral_num" AS text) AS "cadastral_num",
		 "Custom SQL Query"."cancel_date" AS "cancel_date",
		 "Custom SQL Query"."cnt_ods_description" AS "cnt_ods_description",
		 CAST("Custom SQL Query"."deal_num" AS text) AS "deal_num",
		 "Custom SQL Query"."deal_price_amt" AS "deal_price_amt",
		 "Custom SQL Query"."escrow" AS "escrow",
		 "Custom SQL Query"."living" AS "living",
		 CAST("Custom SQL Query"."region_code" AS text) AS "region_code",
		 "Custom SQL Query"."registration_date" AS "registration_date",
		 CAST("Custom SQL Query"."subject_description" AS text) AS "subject_description",
		 "Custom SQL Query"."sum_ods_is_null" AS "sum_ods_is_null"
	FROM 
		(SELECT DISTINCT
			ON (deal_num) cadastral_num, region_code, registration_date, cancel_date, deal_num, deal_price_amt, subject_description, living, escrow, cadastral_doc_addition_txt, agg_ods_description, cnt_ods_description, sum_ods_is_null
		FROM 
			(SELECT DISTINCT
				ON (cadastral_doc_addition_txt, cadastral_num, registration_date) cadastral_num, region_code, registration_date, cancel_date, deal_num, subject_description, living, escrow, cadastral_doc_addition_txt, max(deal_price_amt) AS deal_price_amt, array_agg(ods_description) AS agg_ods_description, count(*) AS cnt_ods_description, sum(ods_is_null) AS sum_ods_is_null
			FROM 
				(SELECT DISTINCT
					ON (ods_description, region_code, wk_dst) *
				FROM 
					(SELECT cadastral_num,
		 d.registration_date,
		 d.cancel_date,
		 d.region_code,
		 d.deal_num,
		 cadastral_doc_addition_txt,
		 deal_price_amt,
		 deal_price_txt,
		 d.subject_description,
		 deal_condition_txt,
		 coalesce(ods_description,
		 subject_description) AS ods_description,
		
						CASE
						WHEN ods_description IS NULL THEN
						1
						ELSE 0
						END AS ods_is_null,
						CASE
						WHEN upper(subject_description) LIKE '%КВАРТИР%' THEN
						1
						ELSE 0
						END AS living,
						CASE
						WHEN (upper(d.deal_condition_txt) LIKE '%ЭСКРОУ%'
							OR upper(d.deal_condition_txt) LIKE '%ЭКСРОУ%'
							OR upper(d.deal_condition_txt) LIKE '%ESCROW%') THEN
						1
						ELSE 0
						END AS escrow, to_char(d.registration_date + 1, 'YYYY-WW') AS wk_dst
					FROM dl_egrn_rosreestr.deal_actual d
					INNER JOIN dl_egrn_rosreestr.cadastral_doc_actual d2
						ON d.bkh = d2.reference_bkh
					LEFT JOIN dl_egrn_rosreestr.realty_actual d3
						ON d3.bkh = d.realty_bkh
					LEFT JOIN dl_egrn_rosreestr.share_subject_description_actual d4
						ON d.bkh = d4.reference_bkh
					LEFT JOIN dl_egrn_rosreestr.realty_address_actual d5
						ON d3.bkh = d5.realty_bkh
					LEFT JOIN dl_egrn_rosreestr.dct_region_actual d6
						ON d6.bkh = d5.realty_region_bkh
					WHERE d.registration_date > '2020-01-01'
							AND cadastral_doc_type_bkh = '6538d008-0e48-9b8c-10a0-d228ec21f9ec'
					ORDER BY  cadastral_num, deal_num, ods_description, cadastral_doc_addition_txt) ord_t) t
					GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9
					ORDER BY  cadastral_doc_addition_txt, cadastral_num) tt
					ORDER BY  deal_num, cadastral_num) "Custom SQL Query" ) t; 
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
