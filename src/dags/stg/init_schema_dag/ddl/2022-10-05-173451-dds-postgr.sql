CREATE TABLE IF NOT EXISTS dds.dm_users (
    id int4 NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    user_id VARCHAR NOT NULL,
    user_name VARCHAR NOT NULL,
    user_login VARCHAR NOT NULL
);


CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
	id int4 NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	restaurant_id varchar NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL
);


CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id int4 NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	ts timestamp,
    ts_year int,
    ts_month int,
    ts_day int,
    ts_date varchar NOT NULL,
    ts_time varchar NOT NULL 
);


CREATE TABLE IF NOT EXISTS dds.dm_products (
	id int4 NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	product_id VARCHAR NOT NULL,
    product_name text NOT NULL,
    product_price numeric NOT NULL,
    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL,
    restaurant_id int NOT NULL,
    CONSTRAINT dm_products_restaurant_id_fkey 
        FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id) 
);
CREATE INDEX IF NOT EXISTS idx_dm_products_id 
    ON dds.dm_products USING btree (id);



CREATE TABLE IF NOT EXISTS dds.dm_orders(
	id int4 NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	order_key varchar NOT NULL,
    order_status varchar NOT NULL,
    restaurant_id INTEGER NOT NULL,
    timestamp_id INTEGER ,
    user_id INTEGER NOT NULL,
    CONSTRAINT dm_orders_restaurant_id_fkey 
        FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT dm_orders_timestamp_id_fkey 
        FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
    CONSTRAINT dm_orders_user_id_fkey 
        FOREIGN KEY (user_id) REFERENCES dds.dm_users(id)
);
CREATE INDEX IF NOT EXISTS idx_dm_orders_id 
    ON dds.dm_orders USING btree (id);


CREATE TABLE IF NOT EXISTS dds.dm_curiers (
    id INTEGER PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL
);


CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
	id int4  NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	product_id INTEGER NOT NULL,
	order_id INTEGER NOT NULL,
	count int4 NOT NULL,
	price numeric(19, 5) NOT NULL,
	total_sum numeric(19, 5) NOT NULL,
	bonus_payment numeric(19, 5) NOT NULL,
	bonus_grant numeric(19, 5) NOT NULL,
	CONSTRAINT fct_product_sales_order_id_fkey 
        FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
	CONSTRAINT fct_product_sales_product_id_fkey 
        FOREIGN KEY (product_id) REFERENCES dds.dm_products(id)
);

CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
	id SERIAL PRIMARY KEY,
    delivery_id VARCHAR NOT NULL,
	order_id INTEGER NOT NULL,
    order_ts timestamp NOT NULL,
    courier_id INTEGER NOT NULL,
    rate NUMERIC(2, 1) NOT NULL,
    sum NUMERIC(19, 5) NOT NULL,
    tip_sum NUMERIC(19, 5) NOT NULL,
    CONSTRAINT fct_deliveries_order_id_fkey 
        FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT fct_deliveries_courier_id_fkey
        FOREIGN KEY (courier_id) REFERENCES dds.dm_curiers(id)
);




