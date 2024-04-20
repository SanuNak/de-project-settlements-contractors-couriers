
-- Создание таблиц из Mongo db , подсистема заказов

CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);


-- Создание таблиц из PG db , подсистема бонусов

CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
    id INTEGER NOT NULL PRIMARY KEY,
    order_user_id VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bonussystem_users_order_user_id ON stg.bonussystem_users USING btree (order_user_id);

CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    bonus_percent NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (bonus_percent >= 0),
    min_payment_threshold NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (min_payment_threshold >= 0)
);
CREATE INDEX IF NOT EXISTS idx_bonussystem_ranks__name ON stg.bonussystem_ranks USING btree (name);

CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
	id INTEGER NOT NULL PRIMARY KEY,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);

-- Создание таблиц из API, подсистема доставки 


CREATE TABLE IF NOT EXISTS stg.deliveryssystem_couriers (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    courier_id VARCHAR NOT NULL,
    "name" VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deliveryssystem_couriers_name ON stg.deliveryssystem_couriers USING btree (name);

CREATE TABLE IF NOT EXISTS stg.deliveryssystem_delivery (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    order_id VARCHAR NOT NULL, 
    order_ts timestamp NOT NULL, 
    delivery_id VARCHAR NOT NULL, 
    courier_id VARCHAR NOT NULL, 
    "address" TEXT NOT NULL, 
    delivery_ts timestamp NOT NULL, 
    rate INT NOT NULL, 
    sum NUMERIC(19, 5) NOT NULL, 
    tip_sum NUMERIC(19, 5) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deliveryssystem_delivery__name ON stg.bonussystem_ranks USING btree (name);
