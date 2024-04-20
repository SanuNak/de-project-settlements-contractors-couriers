from logging import Logger
from typing import List
from datetime import datetime

from airflow.operators.python import get_current_context

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str
from lib.settings_repository import EtlSetting, EtlSettingsRepository


class DmReportOriginObj(BaseModel):
    restaurant_id: int
    restaurant_name: str
    settlement_date: datetime
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float



class DmReportsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_reports(self, dm_report_threshold: str, current_day: str ) -> List[DmReportOriginObj]:
        with self._db.client().cursor(row_factory=class_row(DmReportOriginObj)) as cur:
            cur.execute(
                f"""
                    SELECT 
                        dmo.restaurant_id, 
                        dr.restaurant_name, 
                        dt.ts_date::timestamp AS settlement_date, 
                        SUM(fps.count) AS orders_count, 
                        SUM(fps.total_sum) AS orders_total_sum,
                        SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
                        SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
                        SUM(fps.total_sum * 0.25) AS order_processing_fee,
                        SUM(fps.total_sum - fps.bonus_payment - fps.total_sum * 0.25) AS restaurant_reward_sum
                    FROM dds.fct_product_sales fps
                    INNER JOIN dds.dm_orders dmo 
                        ON fps.order_id = dmo.id
                    INNER JOIN dds.dm_restaurants dr 
                        ON dmo.restaurant_id = dr.id
                    INNER JOIN dds.dm_timestamps dt 
                        ON dmo.timestamp_id = dt.id
                    WHERE dmo.order_status = 'CLOSED'
                        AND dt.ts_date::timestamp::date = '{ current_day }'::timestamp::date
                    GROUP BY dmo.restaurant_id, dr.restaurant_name, dt.ts_date
                    ORDER BY  dt.ts_date DESC, dmo.restaurant_id; --Обрабатываем пачку объектов.
                """, {}
            )
            objs = cur.fetchall() 
        return objs


class DmReportsDestRepository:
    def clear_table(self,
                conn: Connection, 
                current_day: str):
        with conn.cursor() as cur:
            cur.execute(
                f""" DELETE FROM cdm.dm_settlement_report AS dsr
                    WHERE DATE_TRUNC('day', dsr.settlement_date) = '{ current_day }'::timestamp::date;
                """
            )

    def insert_to_db(self, conn: Connection, dm_reports: DmReportOriginObj) -> None:
        # Сюда данные попадают уже в формате DmReportOriginObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(
                        restaurant_id,
                        restaurant_name,
                        settlement_date,
                        orders_count,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum
                        )
                    VALUES (%(restaurant_id)s, 
                            %(restaurant_name)s, 
                            %(settlement_date)s, 
                            %(orders_count)s, 
                            %(orders_total_sum)s,
                            %(orders_bonus_payment_sum)s,
                            %(orders_bonus_granted_sum)s,
                            %(order_processing_fee)s,
                            %(restaurant_reward_sum)s)

                """,
                {
                    "restaurant_id": dm_reports.restaurant_id,
                    "restaurant_name": dm_reports.restaurant_name,
                    "settlement_date": dm_reports.settlement_date,
                    "orders_count": dm_reports.orders_count,
                    "orders_total_sum": dm_reports.orders_total_sum,
                    "orders_bonus_payment_sum": dm_reports.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": dm_reports.orders_bonus_granted_sum,
                    "order_processing_fee": dm_reports.order_processing_fee,
                    "restaurant_reward_sum": dm_reports.restaurant_reward_sum,
                },
            )

class DmReportLoader:
    WF_KEY = "cdm_dm_settlement_report_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 2000  # Загружаем пачками
    SHEMA_TABLE = 'cdm.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmReportsOriginRepository(pg_origin)
        self.cdm = DmReportsDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log
    
        self.context = get_current_context()
        self.current_day = self.context["ds"] # Определяем дуту для выгрузки данных за месяц в формате YYYY-MM-DD
        self.prev_day = self.context["prev_ds"]


    def parse_of_data(self, raws: List[DmReportOriginObj]) -> List[DmReportOriginObj]:
        res = []
        
        for r in raws:
            t = DmReportOriginObj(
                                restaurant_id=r.restaurant_id,
                                restaurant_name=r.restaurant_name,
                                settlement_date=r.settlement_date,
                                orders_count=r.orders_count,
                                orders_total_sum=r.orders_total_sum,
                                orders_bonus_payment_sum=r.orders_bonus_payment_sum,
                                orders_bonus_granted_sum=r.orders_bonus_granted_sum,
                                order_processing_fee=r.order_processing_fee,
                                restaurant_reward_sum=r.restaurant_reward_sum
                                )

            res.append(t)
        return res


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
                                        workflow_settings={self.LAST_LOADED_ID_KEY: self.prev_day})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_reports(last_loaded, self.current_day)


            # выбираем функцию для парсинга в зависимости от вида таблицы
            data_to_load = self.parse_of_data(load_queue)
            self.log.info(f"Found {len(data_to_load)} dm_reports to load.")

            if not data_to_load:
                self.log.info("Quitting.")
                return

            # Чистим таблицу перед сохранением, если уже есть данные за указанный период
            self.cdm.clear_table(conn, self.current_day)


            # Сохраняем объекты в базу dwh.
            for row in data_to_load:
                self.cdm.insert_to_db(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = self.current_day
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


