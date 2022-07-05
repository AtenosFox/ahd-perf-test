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
    def friquent_query_1(self):
        self.client.execute_query(self.conn_string,
                                  '''
                                  SELECT count(*)
FROM 
	(SELECT spark_id,
		 inn,
		 ogrn,
		 kpp,
		 okfs_code,
		 okfs_name,
		 okopf_name,
		 full_name_rus,
		 short_name_rus,
		 status_name,
		 first_registration_date,
		 revenue,
		 net_profit,
		 net_assets,
		 cast(net_profit_flag AS boolean) net_profit_flag,
		 cast(net_assets_flag AS boolean) net_assets_flag,
		 cast(debt_flag AS boolean) debt_flag,
		 cast(rev_loss_flag AS boolean) rev_loss_flag,
		 cast(tax_debt_flag AS boolean) tax_debt_flag,
		 cast(fr_acc_flag AS boolean) fr_acc_flag,
		 cast(bad_sup_flag AS boolean) bad_sup_flag,
		 cast(inv_add_flag AS boolean) inv_add_flag,
		 cast(cur_status_flag AS boolean) cur_status_flag,
		 cast(age_flag AS boolean) age_flag,
		 cast(ben_chg_flag AS boolean) ben_chg_flag,
		 cast(gov_chg_flag AS boolean) gov_chg_flag,
		 cast(dsq_flag AS boolean) dsq_flag,
		 cast(abs_rep_flag AS boolean) abs_rep_flag,
		 cast(inv_fnd_flag AS boolean) inv_fnd_flag,
		 cast(inv_gov_flag AS boolean) inv_gov_flag,
		 cast(bl_flag AS boolean) bl_flag,
		 cast(bp_flag AS boolean) bp_flag,
		 cast(arb_claim_flag AS boolean) arb_claim_flag,
		 cast(arb_def_flag AS boolean) arb_def_flag,
		 cast(staff_number_flag AS boolean) staff_number_flag,
		 cast(rkn_license_flag AS boolean) rkn_license_flag,
		 report_year,
		 (
		CASE
		WHEN check_result = 'RED FLAG' THEN
		FALSE
		WHEN check_result = 'GREEN FLAG' THEN
		TRUE
		WHEN check_result = 'APPROVAL' THEN
		TRUE END) check_res, check_result, lim_calc
	FROM snbx_creditroad.kaa_fin_revision_function_by_inn ('7802411618', '0') t) a;  
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
