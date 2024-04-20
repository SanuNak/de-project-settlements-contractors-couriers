from logging import Logger
from typing import List

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from airflow.operators.python import get_current_context

from lib import PgConnect
from lib.dict_util import json2str

from dags.stg.api_deliverys_system_dag.api_reader import ApiConnect
from lib.settings_repository import EtlSetting, EtlSettingsRepository


class CurierObj(BaseModel):
    courier_id: str
    name: str


class CuriersOriginRepository:
    def __init__(self) -> None:
        pass

    def list_curiers(self, sort:str, threshold: int, limit: int) -> List[CurierObj]:
        x = ApiConnect('couriers', sort, limit, threshold)
        x.client()

        return x.client()


class CurierDestRepository:
    def insert_curier(self, conn: Connection, curier: CurierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliveryssystem_couriers(courier_id, name)
                    VALUES (%(_id)s, %(name)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        name = EXCLUDED.name;
                """,
                {
                    "_id": curier['_id'],
                    "name": curier['name']
                },
            )


class CuriersLoader:
    WF_KEY = "stg__from_api_deliveryssystem_couriers_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 200  # инкрементальная загрузка рангов.
    SHEMA_TABLE = 'stg.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CuriersOriginRepository()
        self.stg = CurierDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log


    def load_curiers(self):
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
            load_queue = self.origin.list_curiers(sort="id", threshold=last_loaded, limit=self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} curiers to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for curier in load_queue:
                self.stg.insert_curier(conn, curier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.

            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
