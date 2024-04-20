import logging
import pendulum

from lib import ConnectionBuilder

from airflow.decorators import dag, task

from dags.stg.api_deliverys_system_dag.couriers_loader import CuriersLoader
from dags.stg.api_deliverys_system_dag.deliveries_loader import DeliverysLoader



log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/3 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 7, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['project5', 'stg', 'origin', 'api'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def project5_stg_deliverys_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="curiers_load")
    def load_curiers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CuriersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_curiers()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DeliverysLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliverys()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    curiers_dict = load_curiers()
    deliveries_dict = load_deliveries()


    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    curiers_dict >> deliveries_dict 


stg_deliverys_system_dag = project5_stg_deliverys_system_dag()
