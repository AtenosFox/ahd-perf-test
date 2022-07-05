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
    def long_query_6(self):
        self.client.execute_query(self.conn_string,
                                '''
                                SELECT count(*)
FROM (WITH all_data AS 
	(SELECT a.src_cian_id --id объявления ,
		 agent.realty_agency_name --агентство недвижимости (банк дом рф и дом рф) ,
		 a.price_amt --стоимость общая ,
		 a.total_area_amt --площадь ,
		 a.room_cnt --кол-во комнат ,
		 a.housing_complex_name --жк ,
		 stat.src_update_date valid_from --действует от (раньше было a.valid_from из таблицы actual,
		 для табло такое же название надо оставить) ,
		 MAX(stat.src_update_date)
		OVER (PARTITION BY a.src_cian_id) last_valid_from --последнее изменение -- ИЛИ BKH --, LAG(a.valid_from)
		OVER (PARTITION BY a.src_cian_id
	ORDER BY  a.valid_from) prev_valid_date --, LEAD(a.valid_from)
		OVER (PARTITION BY a.src_cian_id
	ORDER BY  a.valid_from) next_valid_date , a.bkh, status.status_name "Исторический статус", adr.user_address_txt "Исторический адрес" --, COALESCE(LAG(a.price_amt, 1)
		OVER (PARTITION BY a.src_cian_id, a.bkh
	ORDER BY  a.valid_from), 0) past_price
	FROM dl_cian.advert a
	JOIN dl_cian.advert_statistic stat
		ON stat.deleted_flag = FALSE
			AND stat.advert_bkh = a.bkh
			AND stat.valid_from = a.valid_from
	JOIN dl_cian.dct_realty_agency agent
		ON agent.bkh = a.realty_agency_bkh
			AND agent.deleted_flag = FALSE
			AND agent.realty_agency_name IN ('Банк ДОМ.РФ', 'ДОМ.РФ')
	JOIN dl_cian.dct_status status
		ON stat.deleted_flag = FALSE
			AND status.bkh = a.status_bkh
	LEFT JOIN dl_cian.address adr
		ON adr.deleted_flag = FALSE
			AND adr.src_realty_id = a.src_realty_id
	WHERE a.deleted_flag = FALSE ),
		 distinct_price AS ( --объявления с изменениями ценыSELECT a.src_cian_id
	FROM all_data a
	GROUP BY  a.src_cian_id
	HAVING COUNT(DISTINCT price_amt) > 1 --может меняться не только цена,
		 но и,
		 например,
		 статус,
		 поэтому DISTINCT ),
		 real_adverts AS ( --Предложение Бородина А. до внедрения изменений в ключ ЦИАНSELECT advert_bkh
	FROM 
		(SELECT aa.bkh advert_bkh,
		 RANK()
			OVER (PARTITION BY aa.src_cian_id
		ORDER BY  stat.src_update_date DESC, stat.src_upload_date DESC) rn
		FROM dl_cian.advert_actual aa
		JOIN dl_cian.advert_statistic_actual stat
			ON stat.advert_bkh = aa.bkh
		JOIN dl_cian.dct_realty_agency_actual agent
			ON agent.bkh = aa.realty_agency_bkh
				AND agent.realty_agency_name IN ('Банк ДОМ.РФ', 'ДОМ.РФ')) t
		WHERE rn = 1 )
		SELECT d.* --,
		 LTRIM(COALESCE(to_char(date_part('day', d.valid_from - d.next_valid_date), '9999'), 'Новое объявление'), ' ') ,
			CASE status.status_name
			WHEN 'активно' THEN
			CURRENT_DATE - aa.create_date
			ELSE d.valid_from - aa.create_date
			END srok, status.status_name, aa.price_amt actual_price_amt, aa.total_area_amt actual_total_area_amt, aa.price_sqm_amt actual_price_sqm_amt, aa.create_date --дата создания , aa.advert_url_txt --адрес (ссылка) , bld_tp.building_type_name --тип здания , aa.subway_minute_amt --время до станции метро , aa.prepay_month_cnt --месяцев предоплаты , aa.deposit_amt --депозит , cat.advert_category_name --категория объявления , ad_stat.view_cnt --кол-во просмотров , adr.region_name --регион , adr.settlement_name --город , adr.user_address_txt --адрес, введённый пользователем ,
			CASE
			WHEN dis.src_cian_id IS NOT NULL THEN
			'Да'
			ELSE 'Нет'
			END change_price
		FROM dl_cian.advert_actual aa
		JOIN real_adverts r
			ON r.advert_bkh = aa.bkh
		LEFT JOIN all_data d
			ON aa.src_cian_id = d.src_cian_id --aa.bkh = d.bkh
		JOIN dl_cian.dct_status_actual status
			ON status.bkh = aa.status_bkh
		JOIN dl_cian.dct_advert_category_actual cat
			ON aa.advert_category_bkh = cat.bkh
		LEFT JOIN dl_cian.advert_statistic_actual ad_stat
			ON ad_stat.advert_bkh = aa.bkh
		LEFT JOIN dl_cian.address_actual adr
			ON adr.src_realty_id = aa.src_realty_id
		LEFT JOIN dl_cian.dct_building_type_actual bld_tp
			ON bld_tp.bkh = aa.building_type_bkh
		LEFT JOIN distinct_price dis
			ON dis.src_cian_id = aa.src_cian_id
		WHERE aa.src_cian_id = 246598838) a;
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
