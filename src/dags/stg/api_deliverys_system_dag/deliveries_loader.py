from logging import Logger
from typing import List
from datetime import datetime

from psycopg import Connection
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str

from dags.stg.api_deliverys_system_dag.api_reader import ApiConnect
from lib.settings_repository import EtlSetting, EtlSettingsRepository


class DeliveryObj(BaseModel):
    order_id: str
    order_ts: datetime
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: int
    sum: float
    tip_sum: float


class DeliverysOriginRepository:
    def __init__(self) -> None:
        pass

    def list_deliverys(self, sort:str, threshold: int, limit: int)-> List[DeliveryObj]:
        x = ApiConnect('deliveries', sort, limit , threshold)
        x.client()

        return x.client()


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliveryssystem_delivery
                        (order_id, 
                        order_ts, 
                        delivery_id, 
                        courier_id, 
                        address, 
                        delivery_ts, 
                        rate, 
                        sum, 
                        tip_sum)
                    VALUES (%(order_id)s, 
                            %(order_ts)s, 
                            %(delivery_id)s, 
                            %(courier_id)s, 
                            %(address)s, 
                            %(delivery_ts)s, 
                            %(rate)s, 
                            %(sum)s, 
                            %(tip_sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "order_id": delivery["order_id"],
                    "order_ts": delivery["order_ts"],
                    "delivery_id": delivery["delivery_id"],
                    "courier_id": delivery["courier_id"],
                    "address": delivery["address"],
                    "delivery_ts": delivery["delivery_ts"],
                    "rate": delivery["rate"],
                    "sum": delivery["sum"],
                    "tip_sum": delivery["tip_sum"]
                },
            )


class DeliverysLoader:
    WF_KEY = "stg_from_api_deliveryssystem_delivery_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.
    SHEMA_TABLE = 'stg.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliverysOriginRepository()
        self.stg = DeliveryDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

    def load_deliverys(self):
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
            load_queue = self.origin.list_deliverys(sort="order_ts", threshold=last_loaded, limit=self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliverys to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.

            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
