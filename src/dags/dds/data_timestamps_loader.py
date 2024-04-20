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

class DmTimestampOriginObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class DmTimestampDestObj(BaseModel):
    id: int
    ts: datetime
    ts_year: int
    ts_month: int
    ts_day: int
    ts_time: str
    ts_date: str


class DmTimestampOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamp(self, dm_timestamp_threshold: str, limit: int) -> List[DmTimestampOriginObj]:
        with self._db.client().cursor(row_factory=class_row(DmTimestampOriginObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": dm_timestamp_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmTimestampsDestRepository:

    def insert_to_db(self, conn: Connection, dm_timestamps: DmTimestampDestObj) -> None:
        # Сюда данные попадают уже в формате DmTimestampDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, ts_year, ts_month, ts_day, ts_time, ts_date)
                    VALUES (%(ts)s, %(ts_year)s, %(ts_month)s, %(ts_day)s, %(ts_time)s, %(ts_date)s);
                """,
                {
                    "ts": dm_timestamps.ts,
                    "ts_year": dm_timestamps.ts_year,
                    "ts_month": dm_timestamps.ts_month,
                    "ts_day": dm_timestamps.ts_day,
                    "ts_time": dm_timestamps.ts_time,
                    "ts_date": dm_timestamps.ts_date
                },
            )


class DmTimestampLoader:
    WF_KEY = "dds_dm_timestamps_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 1000  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmTimestampOriginRepository(pg_origin)
        self.dds = DmTimestampsDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log


    def parse_of_data(self, raws: List[DmTimestampDestObj]) -> List[DmTimestampDestObj]:
        res = []
        for r in raws:
            json_of_data = json.loads(r.object_value)
            if json_of_data['final_status'] == "CLOSED" or json_of_data['final_status'] == "CANCELLED":
                date_time = datetime.strptime(json_of_data['ts_date'], "%Y-%m-%d %H:%M:%S")
                t = DmTimestampDestObj(
                                        id = r.id,
                                        ts = date_time,
                                        ts_year = date_time.ts_year,
                                        ts_month = date_time.ts_month,
                                        ts_day = date_time.ts_day,
                                        ts_time = datetime.strftime(date_time, "%H:%M:%S"),
                                        ts_date = datetime.strftime(date_time, "%Y-%m-%d"),
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
            load_queue = self.origin.list_timestamp(last_loaded, self.BATCH_LIMIT)

            # выбираем функцию для парсинга в зависимости от вида таблицы
            data_to_load = self.parse_of_data(load_queue)
            self.log.info(f"Found {len(data_to_load)} dm_timestamp to load.")

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


