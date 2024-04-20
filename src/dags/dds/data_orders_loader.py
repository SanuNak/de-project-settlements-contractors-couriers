from logging import Logger
from typing import List
from datetime import datetime
import json

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str

from lib.settings_repository import EtlSetting, EtlSettingsRepository


class DmOrderOriginObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class DmOrderDestObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int


class DmBonusEvObj(BaseModel):
    id: int
    event_type: str
    event_ts: datetime
    event_value: str

class DmOrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, dm_order_threshold: str, limit: int) -> List[DmOrderOriginObj]:
        with self._db.client().cursor(row_factory=class_row(DmOrderOriginObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": dm_order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


    def list_orders_bonus_events(self, dm_order_threshold: str, limit: int) -> List[DmBonusEvObj]:
        with self._db.client().cursor(row_factory=class_row(DmBonusEvObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_type, event_ts, event_value
                    FROM stg.bonussystem_events
                    WHERE id > %(threshold)s
                        AND event_type = 'bonus_transaction'
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": dm_order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

class DmOrdersDestRepository:

    def insert_to_db(self, conn: Connection, dm_orders: DmOrderDestObj) -> None:
        # Сюда данные попадают уже в формате DmOrderDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s);
                """,
                {
                    "order_key": dm_orders.order_key,
                    "order_status": dm_orders.order_status,
                    "restaurant_id": dm_orders.restaurant_id,
                    "timestamp_id": dm_orders.timestamp_id,
                    "user_id": dm_orders.user_id,
                },
            )


class DmOrderLoader:
    WF_KEY = "dds_dm_orders_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmOrdersOriginRepository(pg_origin)
        self.dds = DmOrdersDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

    def get_restaurant_id(self, restaurant_id) -> int:
        with self.pg_dest.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_restaurants as r
                    WHERE r.restaurant_id = %(restaurant_id)s
                """,
                {
                    "restaurant_id": restaurant_id,
                },
            )
            objs = cur.fetchone()[0]
        return objs


    def get_timestamp_id(self, timestamp_id) -> int:
        with self.pg_dest.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_timestamps as t
                    WHERE t.ts = %(timestamp_id)s
                """,
                {
                    "timestamp_id": timestamp_id,
                },
            )
            objs = cur.fetchone()[0]
        return objs
    

    def get_user_id(self, user_id) -> int:
        with self.pg_dest.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_users as u
                    WHERE u.user_id = %(user_id)s
                """,
                {
                    "user_id": user_id,
                },
            )
            objs = cur.fetchone()[0]
        return objs
    

    def parse_of_data(self, raws: List[DmOrderDestObj]) -> List[DmOrderDestObj]:
        res = []
        for r in raws:
            json_of_data = json.loads(r.object_value)
            t = DmOrderDestObj(
                                    id = r.id,
                                    order_key=json_of_data['_id'],
                                    order_status=json_of_data['final_status'],
                                    restaurant_id=self.get_restaurant_id(json_of_data["restaurant"]["id"]),
                                    timestamp_id=self.get_timestamp_id(json_of_data["update_ts"]),
                                    user_id=self.get_user_id(json_of_data["user"]["id"]),
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
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            

            # выбираем функцию для парсинга в зависимости от вида таблицы
            data_to_load = self.parse_of_data(load_queue)
            self.log.info(f"Found {len(data_to_load)} dm_orders to load.")

            if not data_to_load:
                self.log.info("Quitting.")
                return


            # Сохраняем объекты в базу dwh.
            for row in data_to_load:
                self.dds.insert_to_db(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in data_to_load])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


