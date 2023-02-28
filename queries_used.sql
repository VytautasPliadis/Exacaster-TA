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
            (customer_id)
SELECT DISTINCT customer_id
FROM   usage;

INSERT INTO rate_plans
            (rate_plan_id)
SELECT DISTINCT rate_plan_id
FROM   usage; 

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
FROM   usage; 
-- ------------------------------------------------------------------------------------------------------------------------------
DELETE
FROM   events
WHERE  event_start_time < Now() - interval '6 MONTH';

DELETE FROM customers
WHERE customer_id IN (
    SELECT c.customer_id
    FROM customers c
    LEFT JOIN (
        SELECT customer_id, MAX(event_start_time) AS last_event_time
        FROM events
        GROUP BY customer_id
    ) e ON c.customer_id = e.customer_id
    WHERE e.last_event_time < NOW() - INTERVAL '6 MONTH' OR e.customer_id IS NULL
);

DELETE FROM rate_plans
WHERE rate_plan_id IN (
    SELECT rp.rate_plan_id
    FROM rate_plans rp
    LEFT JOIN (
        SELECT rate_plan_id, MAX(event_start_time) AS last_event_time
        FROM events
        GROUP BY rate_plan_id
    ) e ON rp.rate_plan_id = e.rate_plan_id
    WHERE e.last_event_time < NOW() - INTERVAL '6 MONTH' OR e.rate_plan_id IS NULL
);
-- ------------------------------------------------------------------------------------------------------------------------------                            
SELECT cron.schedule('0 0 * * *', 'DELETE FROM usage WHERE event_start_time < NOW() - INTERVAL ''6 months''');