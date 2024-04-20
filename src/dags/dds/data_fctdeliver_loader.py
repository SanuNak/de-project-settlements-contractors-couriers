from logging import Logger
from typing import List, Union
from datetime import datetime

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str

from lib.settings_repository import EtlSetting, EtlSettingsRepository


class DmDeliveryOriginObj(BaseModel):
    id: int
    delivery_id: str
    order_id: str
    order_ts: datetime
    courier_id: str
    rate: float
    sum: float
    tip_sum: float


class DmDeliveryDestObj(BaseModel):
    id: int
    delivery_id: str
    order_id: str
    order_ts: datetime
    courier_id: str
    rate: float
    sum: float
    tip_sum: float



class DmDeliverysOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliverys(self, dm_delivery_threshold: str, limit: int) -> Union[List[DmDeliveryOriginObj], None]:
        with self._db.client().cursor(row_factory=class_row(DmDeliveryOriginObj)) as cur:
            cur.execute(
                """
                    WITH dmord AS (
                                    SELECT id, order_key
                                    FROM dds.dm_orders
                                    ),
                        dmcour AS (
                                    SELECT id, courier_id
                                    FROM dds.dm_curiers dc 
                                    )
                    SELECT 
                        dd.id, 
                        delivery_id, 
                        dmord.id AS order_id,
                        order_ts,
                        dmcour.id AS courier_id,
                        rate,
                        sum,
                        tip_sum
                    FROM stg.deliveryssystem_delivery dd
                    LEFT JOIN dmord ON dd.order_id = dmord.order_key
                    LEFT JOIN dmcour ON dd.courier_id = dmcour.courier_id
                    WHERE dd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                        AND dmord.id is NOT null
                    ORDER BY dd.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": dm_delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmDeliverysDestRepository:

    def insert_dm_delivery(self, conn: Connection, dm_deliverys: DmDeliveryDestObj) -> None:
        # Сюда данные попадают уже в формате DmDeliveryDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(
                        delivery_id, 
                        order_id,
                        order_ts,
                        courier_id,
                        rate,
                        sum,
                        tip_sum
                    )
                    VALUES (
                        %(delivery_id)s, 
                        %(order_id)s,
                        %(order_ts)s,
                        %(courier_id)s,
                        %(rate)s,
                        %(sum)s,
                        %(tip_sum)s                       
                    )
                    ON CONFLICT (id) DO NOTHING;

                """,
                {
                    "delivery_id": dm_deliverys.delivery_id, 
                    "order_id": dm_deliverys.order_id,
                    "order_ts": dm_deliverys.order_ts,
                    "courier_id": dm_deliverys.courier_id,
                    "rate": dm_deliverys.rate,
                    "sum": dm_deliverys.sum,
                    "tip_sum": dm_deliverys. tip_sum 
                },
            )


class DmFctDelivLoader:
    WF_KEY = "dds_fct_deliveries_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmDeliverysOriginRepository(pg_origin)
        self.dds = DmDeliverysDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log


    def data_load(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliverys(last_loaded, self.BATCH_LIMIT)

            self.log.info(f"Found {len(load_queue)} dm_deliverys to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.dds.insert_dm_delivery(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


