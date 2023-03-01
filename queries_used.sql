CREATE TABLE events
  (
     event_id         SERIAL PRIMARY KEY,
     customer_id      INT,
     event_start_time TIMESTAMP WITH time zone,
     event_type       VARCHAR(5),
     rate_plan_id     INT,
     billing_flag_1   SMALLINT,
     billing_flag_2   BOOLEAN,
     duration         SMALLINT,
     charge           REAL,
     month            VARCHAR(7)
  ); 

CREATE TABLE rate_plans
  (
     rate_plan_id INT PRIMARY KEY
  ); 

CREATE TABLE customers
  (
     customer_id INT PRIMARY KEY
  ); 
-- ------------------------------------------------------------------------------------------------------------------------------
ALTER TABLE events
  ADD CONSTRAINT fk_events_customers FOREIGN KEY (customer_id) REFERENCES
  customers(customer_id);

ALTER TABLE events
  ADD CONSTRAINT fk_events_rate_plans FOREIGN KEY (rate_plan_id) REFERENCES
  rate_plans(rate_plan_id); 
-- ------------------------------------------------------------------------------------------------------------------------------
INSERT INTO customers
  (SELECT DISTINCT customer_id
   FROM landing_zone
   WHERE customer_id NOT IN
       (SELECT DISTINCT customer_id
        FROM customers));

INSERT INTO rate_plans
  (SELECT DISTINCT rate_plan_id
   FROM landing_zone
   WHERE rate_plan_id NOT IN
       (SELECT DISTINCT rate_plan_id
        FROM rate_plans));

INSERT INTO events
            (customer_id,
             event_start_time,
             event_type,
             rate_plan_id,
             billing_flag_1,
             billing_flag_2,
             duration,
             charge,
             month)
SELECT 
       customer_id,
       event_start_time,
       event_type,
       rate_plan_id,
       billing_flag_1,
       billing_flag_2,
       duration,
       charge,
       month
FROM   landing_zone; 
-- ------------------------------------------------------------------------------------------------------------------------------
DELETE
FROM events
WHERE event_start_time < Now() - interval '6 MONTH';

DELETE
FROM customers
WHERE customer_id NOT IN
    (SELECT customer_id
     FROM events);

DELETE
FROM rate_plans
WHERE rate_plan_id NOT IN
    (SELECT rate_plan_id
     FROM events);
-- ------------------------------------------------------------------------------------------------------------------------------                            
SELECT cron.schedule('0 0 * * *', 'DELETE FROM events WHERE event_start_time < NOW() - INTERVAL ''6 months''');
