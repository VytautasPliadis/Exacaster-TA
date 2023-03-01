# Technical Assignment
You started working for "Telco Relax" as the only technical expert in DWH project.
In the project directory there is usage data file in CSV format of the following structure:

Customer ID, Event Start Time, Event Type, Rate Plan ID, 
Billing Flag 1, Billing Flag 2, Duration, Charge, Month

**Business owner sent you the requirements**<br>
This is initial data, load it to database. We will get new data like this every week from our billing provider. And yeah, they usually have poor data quality so we need to send them alert if it’s bad asap.
Product guys want to see usage distribution and number of customers by Service Type and Rate Plan. 
They said they will get more data sources later. Data must be removed after 6 months – legal requirement...and departed for 6 week vacation.

**Your task**<br>
Based on what you have got, design initial data structure for the Data Warehouse. Deploy it on the database of your choice.
Build ETL scripts (preferably using Python) to populate it and check data quality.
Explain how you would implement support and monitoring process.
How do you see the evolution of the DWH project in "Telco Relax"? What will be major organizational and technical challenges?

# TA Solution
<br>**First task**<br>
I chose Postgres for Data Warehouse, mostly because of its open source. PostgreSQL is also known for its stability and reliability. First of all, I created a landing zone table for CSV data. Then with the queries (can be found in the *queries_used.sql*), the rest of the main entities are populated.<br>

Main entities with attributes:<br>
* **customers**<br>
customer_id (primary key)<br>
* **rate_plans**<br>
  rate_plan_id (primary key)<br>
* **events**<br>
  event_id (primary key)<br>
  customer_id (foreign key)<br>
  event_start_time<br>
  event_type<br>
  rate_plan_id (foreign key)<br>
  billing_flag_1<br>
  billing_flag_2<br>
  duration<br>
  charge<br>
  month<br>
  
  
<br>**Second task**<br>
One of the business requirements is to get alert of bad quality of data asap. Quality must be checked before the ingestion process. The data_quality script checks constraints from the *usage.tdda* file and sends an alert email if any issues are found.
To ingest such a large CSV file (1,54 GB) can cause system crashes. I decided to split it into smaller pieces (700KB, 10000 rows for each file). Script *csv_to_postgres* connects to the database and creates the usage table mentioned above, inserts all files in the split_csv folder. If an error occurs in the ingestion process, logs will be created for problematic rows for further investigation, without stopping the process. The CSV file is ingested within 2-3 hours. Split files allow useful monitoring capability *print(f"{count} of {csv_qty} {csv_file}")*.


<br>**Third task**<br>
There are some key steps to implement a support and monitoring process for a data warehouse:
* Define Service Level Agreements (SLAs). SLAs that outline the expected uptime, performance, and response time for the data warehouse. These SLAs should be agreed upon by all stakeholders and communicated to the end-users.
* Set up monitoring tools. To effectively monitor the data warehouse, we should use a combination of monitoring tools that can track key performance indicators (KPIs) and system metrics. These tools could include network monitoring tools, database monitoring tools, and application performance monitoring tools.
* We should develop a set of support processes that outline how issues are reported, investigated, and resolved. These processes should include steps for identifying the root cause of issues and developing and implementing corrective actions.
* It's important to provide training and documentation to end-users on how to use the data warehouse effectively and how to report issues. This will ensure that end-users are equipped to use the system properly and can help reduce the number of support requests.
* Regular maintenance and updates to ensure the data warehouse is running at peak performance. This could include database optimization, server patching, and software upgrades.


<br>**Fourth task**<br>
The evolution of a data warehouse project can be influenced by a variety of factors:
* As the organization grows and changes, the data warehouse may need to be updated to support new business requirements. 
* Data quality and governance becomes more challenging when data volumes increase and data becomes more complex.
* As data volumes and complexity grow, the data warehouse infrastructure needs to be able to scale to handle the increased workload.
* Integrating data from multiple sources can be challenging, especially if the data is coming from disparate systems or if the data is structured differently.


<br>**Fifth task**<br>
One of business requirements to see usage distribution and number of customers by Service Type and Rate Plan. Query for usage distribution:
<pre>
SELECT event_type,
       rate_plan_id,
       SUM(duration) AS total_duration,
       COUNT(DISTINCT customer_id) AS total_customers
FROM events
GROUP BY event_type,
         rate_plan_id;
</pre>


<br>**Sixth task**<br>
Data must be removed after 6 months. Data can be deleted from database manually:
<pre>
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

-- Also we can create a background job that runs periodically to delete 
-- old data from the table. pg_cron extension to schedule the job:

SELECT cron.schedule('0 0 * * *', 'DELETE FROM usage WHERE event_start_time < NOW() - INTERVAL ''6 months''');
</pre>

<br>**Personal observations**<br>
In the process of doing this assignment, I ran into several errors. One of them was the first row value shift from 0 to 0.1 in the billing_flag_2 column, splitting the CSV file. Data profiling and TDDA tools have helped to identify the root cause. The problem was the lack of headers in the data frame.
The second flav was to load all info into a  database and then clear duplicates. I realized that in the long run this would be an unnecessary burden on the database, especially when the data will increase over time.
