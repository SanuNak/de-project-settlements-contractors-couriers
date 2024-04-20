import logging

import pendulum
from airflow.decorators import dag, task

from dags.dds.data_couriers_loader import DmCourierLoader
from dags.dds.data_users_loader import DmUserLoader
from dags.dds.data_restaurnts_loader import DmRestaurantLoader
from dags.dds.data_timestamps_loader import DmTimestampLoader
from dags.dds.data_products_loader import DmProductLoader
from dags.dds.data_orders_loader import DmOrderLoader
from dags.dds.data_fct_loader import DmFctLoader
from dags.dds.data_fctdeliver_loader import DmFctDelivLoader


from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/20 * * * *',  # Задаем расписание выполнения дага - каждый 20 минут.
    start_date=pendulum.datetime(2023, 7, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True,  # Остановлен/запущен при появлении. Сразу запущен.
    
)
def project5_dds_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_curiers_load")
    def load_dm_curiers_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmCourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load("dm_curiers_load")  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_dm_users_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmUserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load("dm_users_load")  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_restaurants_load")
    def load_dm_restaurant_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmRestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="dm_timestamps_load")
    def load_dm_timestamp_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmTimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="dm_products_load")
    def load_dm_product_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="dm_orders_load")
    def load_dm_order_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmOrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="fct_product_sales")
    def load_fct_product_sales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmFctLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="fct_deliveries")
    def load_fct_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmFctDelivLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.



    # Инициализируем объявленные таски.
    dm_curiers = load_dm_curiers_data()
    dm_user_dict = load_dm_users_data()
    dm_restaurant_dict = load_dm_restaurant_data()
    dm_timestamp_dict = load_dm_timestamp_data()
    dm_product_dict = load_dm_product_data()
    dm_order_dict = load_dm_order_data()
    fct_product_sales = load_fct_product_sales()
    fctdeliv_product_sales = load_fct_deliveries()


    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    (
        [dm_curiers, dm_user_dict, dm_restaurant_dict, dm_timestamp_dict] >> 
        dm_product_dict >>  
        dm_order_dict >> 
        [fct_product_sales, fctdeliv_product_sales]
    )

stg_to_dds_dag = project5_dds_load_dag()
