from logging import Logger
from typing import List
from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow.operators.python import get_current_context

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str
from lib.settings_repository import EtlSetting, EtlSettingsRepository


class FactsOriginObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_reward_sum: float
    courier_tips_sum: float


class FactsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_reports(self, dm_report_threshold: str, current_month: str ) -> List[FactsOriginObj]:
        with self._db.client().cursor(row_factory=class_row(FactsOriginObj)) as cur:
            current_month = current_month # Определяем дуту для выгрузки данных за месяц в формате YYYY-MM-01
            cur.execute(
                f"""
                    WITH agr AS (
                                SELECT
                                    fd.courier_id,
                                    dc.courier_name,
                                    dt."ts_year"  AS settlement_year,
                                    dt."ts_month" AS settlement_month,
                                    COUNT(fd.courier_id) AS orders_count,
                                    SUM(fd."sum") AS orders_total_sum,
                                    AVG (rate) AS rate_avg,
                                    SUM(fd."sum") * 0.25 AS order_processing_fee,
                                    SUM(fd.tip_sum) AS courier_tips_sum
                                FROM dds.fct_deliveries fd
                                LEFT JOIN dds.dm_curiers dc ON fd.courier_id = dc.id
                                LEFT JOIN dds.dm_timestamps dt ON date_trunc('minute' , fd.order_ts) = date_trunc('minute' , dt.ts)
	                            WHERE date_trunc('month' , fd.order_ts)::date = '{ current_month }'::timestamp::date
                                GROUP BY fd.courier_id, dc.courier_name, dt."ts_year", dt."ts_month"
                                ),
                        agr2 AS(
                                SELECT
                                    agr.courier_id,
                                    agr.courier_name,
                                    agr.settlement_year,
                                    agr.settlement_month,
                                    agr.orders_count,
                                    agr.orders_total_sum,
                                    agr.rate_avg,
                                    agr.order_processing_fee,
                                    CASE 
                                        WHEN agr.rate_avg < 4 AND agr.orders_total_sum * 0.05 > 100 THEN agr.orders_total_sum * 0.05
                                        WHEN agr.rate_avg < 4 AND agr.orders_total_sum * 0.05 <= 100 THEN 100
                                        WHEN (agr.rate_avg >= 4 AND agr.rate_avg < 4.5) AND agr.orders_total_sum * 0.07 > 150 THEN agr.orders_total_sum * 0.07
                                        WHEN (agr.rate_avg >= 4 AND agr.rate_avg < 4.5) AND agr.orders_total_sum * 0.07 <= 150  THEN 150
                                        WHEN (agr.rate_avg >= 4.5 AND agr.rate_avg < 4.9) AND agr.orders_total_sum * 0.08 > 175 THEN agr.orders_total_sum * 0.08
                                        WHEN (agr.rate_avg >= 4.5 AND agr.rate_avg < 4.9) AND agr.orders_total_sum * 0.08 <= 175 THEN 175
                                        WHEN agr.rate_avg >= 4.9 AND agr.orders_total_sum * 0.1 > 200 THEN agr.orders_total_sum * 0.1
                                        WHEN agr.rate_avg >= 4.9 AND agr.orders_total_sum * 0.1 <= 200 THEN 200
                                    END AS courier_order_sum,
                                    agr.courier_tips_sum
                                FROM agr
                                )
                        SELECT 
                            agr2.courier_id,
                            agr2.courier_name,
                            agr2.settlement_year,
                            agr2.settlement_month,
                            agr2.orders_count,
                            agr2.orders_total_sum,
                            agr2.rate_avg,
                            agr2.order_processing_fee,
                            agr2.courier_order_sum,
                            agr2.courier_tips_sum,
                            agr2.courier_order_sum + agr2.courier_tips_sum * 0.95 AS courier_reward_sum
                        FROM agr2
                        ORDER BY agr2.settlement_year ASC, agr2.settlement_month DESC, agr2.courier_id
                """, {}
            )
            objs = cur.fetchall() 
        return objs


class DestRepository:
    def clear_table(self,
                    conn: Connection, 
                    current_month: str):
        with conn.cursor() as cur:
            cur.execute(
                f""" DELETE FROM cdm.dm_courier_ledger AS dcl
                    WHERE dcl.settlement_year = EXTRACT(year FROM '{ current_month }'::timestamp::date)
                    AND settlement_month = EXTRACT(month FROM '{ current_month }'::timestamp::date);
                """
            )


    def insert_to_db(self, 
                     conn: Connection, 
                     dm_reports: FactsOriginObj) -> None:
        # Сюда данные попадают уже в формате DmReportOriginObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_reward_sum
                        )
                    VALUES (%(courier_id)s, 
                            %(courier_name)s, 
                            %(settlement_year)s, 
                            %(settlement_month)s, 
                            %(orders_count)s,
                            %(orders_total_sum)s,
                            %(rate_avg)s,
                            %(order_processing_fee)s,
                            %(courier_order_sum)s,
                            %(courier_tips_sum)s,
                            %(courier_reward_sum)s);
                """,
                {
                    "courier_id": dm_reports.courier_id,
                    "courier_name": dm_reports.courier_name,
                    "settlement_year": dm_reports.settlement_year,
                    "settlement_month": dm_reports.settlement_month,
                    "orders_count": dm_reports.orders_count,
                    "orders_total_sum": dm_reports.orders_total_sum,
                    "rate_avg": dm_reports.rate_avg,
                    "order_processing_fee": dm_reports.order_processing_fee,
                    "courier_order_sum": dm_reports.courier_order_sum,
                    "courier_tips_sum": dm_reports.courier_tips_sum,
                    "courier_reward_sum": dm_reports.courier_reward_sum
                },
            )

class DmCourierLoader:
    WF_KEY = "cdm_dm_courier_ledger_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    SHEMA_TABLE = 'cdm.srv_wf_settings'
    

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FactsOriginRepository(pg_origin)
        self.cdm = DestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

        self.context = get_current_context()
        self.current_month = self.context["ds"] # Определяем дуту для выгрузки данных за месяц в формате YYYY-MM-01
        self.prev_month = self.context["prev_ds"]

    def data_load(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, 
                                        workflow_key=self.WF_KEY, 
                                        workflow_settings={self.LAST_LOADED_ID_KEY: self.prev_month})
            

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_reports(last_loaded, self.current_month)


            # выбираем функцию для парсинга в зависимости от вида таблицы
            self.log.info(f"Found {len(load_queue)} dm_reports to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            # Чистим таблицу перед сохранением, если уже есть данные за указанный период
            self.cdm.clear_table(conn, self.current_month)

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.cdm.insert_to_db(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = self.current_month
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


