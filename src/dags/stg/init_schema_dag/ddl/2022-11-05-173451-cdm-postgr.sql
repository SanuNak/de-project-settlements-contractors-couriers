CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
	id int4 NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	restaurant_id VARCHAR NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    settlement_date timestamp NOT NULL,
    orders_count int NOT NULL,
    orders_total_sum numeric(19, 5) NOT NULL,
    orders_bonus_payment_sum numeric(19, 5) NOT NULL NOT NULL,
    orders_bonus_granted_sum numeric(19, 5) NOT NULL,
    order_processing_fee numeric(19, 5) NOT NULL,
    restaurant_reward_sum numeric(19, 5) NOT NULL
);


CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id SERIAL PRIMARY KEY,
	courier_id INTEGER NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year int  NOT NULL,
    settlement_month INTEGER NOT NULL,
    orders_count int NOT NULL,
    orders_total_sum numeric(19, 5) NOT NULL,
    rate_avg numeric(2, 1) NOT NULL, 
    order_processing_fee numeric(19, 5) NOT NULL,
    courier_order_sum numeric(19, 5) NOT NULL,
    courier_tips_sum numeric(19, 5) NOT NULL,
    courier_reward_sum numeric(19, 5) NOT NULL,
    CONSTRAINT dm_courier_ledger_courier_id_fkey 
        FOREIGN KEY (courier_id) REFERENCES dds.dm_curiers(id)
);