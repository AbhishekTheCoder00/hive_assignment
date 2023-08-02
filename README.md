# hive_assignment
hive assignment 


1. Download vechile sales data -> https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv

2. Store raw data into hdfs location
/home/databasess/sales_order_data.csv' store the dataset in docker home folder


3. Create a internal hive table "sales_order_csv" which will store csv data sales_order_csv .. make sure to skip header row while creating table
CREATE TABLE sales_orders (
       ordernumber INT,
       quantityordered INT,
       priceeach FLOAT,
       orderlinenumber INT,
       sales FLOAT,
       status STRING,
       qtr_id INT,
       month_id INT,
       year_id INT,
       productline STRING,
       msrp INT,
       productcode STRING,
       phone STRING,
       city STRING,
       state STRING,
       postalcode STRING,
       country STRING,
       territory STRING,
       contactlastname STRING,
       contactfirstname STRING,
       dealsize STRING
     )
    row format delimited
    fields terminated by',';
   


4. Load data from hdfs path into "sales_order_csv" 
load data local inpath'file:///home/databasess/sales_order_data.csv' into table sales_orders;


5. Create an internal hive table which will store data in ORC format "sales_order_orc"
CREATE TABLE sales_orders_orc(
  ordernumber INT,
  quantityordered INT,
  priceeach FLOAT,
  orderlinenumber INT,
  sales FLOAT,
  status STRING,
  qtr_id INT,
  month_id INT,
  year_id INT,
  productline STRING,
  msrp INT,
  productcode STRING,
  phone STRING,
  city STRING,
  state STRING,
  postalcode STRING,
  country STRING,
  territory STRING,
  contactlastname STRING,
  contactfirstname STRING,
  dealsize STRING
)
stored as orc;

6. Load data from "sales_order_csv" into "sales_order_orc"
INSERT INTO TABLE sales_orders_orc
SELECT
  CAST(ordernumber AS INT),
  CAST(quantityordered AS INT),
  CAST(priceeach AS FLOAT),
  CAST(orderlinenumber AS INT),
  CAST(sales AS FLOAT),
  status,
  CAST(qtr_id AS INT),
  CAST(month_id AS INT),
  CAST(year_id AS INT),
  productline,
  CAST(msrp AS INT),
  productcode,
  phone,
  city,
  state,
  postalcode,
  country,
  territory,
  contactlastname,
  contactfirstname,
  dealsize
FROM sales_orders;

a. Calculatye total sales per year
select year_id, sum(sales) as total_sales from sales_orders group by year_id order by year_id;


b. Find a product for which maximum orders were placed
select productline, productcode, max(quantityordered) as max_product_order from sales_orders group by productline,productcode order by max_product_order desc; 

c. Calculate the total sales for each quarter
SELECT year_id, quarter(DATE_FORMAT(CAST(CONCAT(year_id, '-', month_id, '-01') AS DATE), 'yyyy-MM-dd')) AS quarter, SUM(sales) AS total_sales FROM sales_orders GROUP BY year_id, quarter(DATE_FORMAT(CAST(CONCAT(year_id, '-', month_id, '-01') AS DATE), 'yyyy-MM-dd')) ORDER BY year_id, quarter;

d. In which quarter sales was minimum
WITH sales_by_quarter AS (SELECT year_id, quarter(DATE_FORMAT(CAST(CONCAT(year_id, '-', month_id, '-01') AS DATE), 'yyyy-MM-dd')) AS quarter, SUM(sales) AS total_sales FROM sales_orders GROUP BY year_id, quarter(DATE_FORMAT(CAST(CONCAT(year_id, '-', month_id, '-01') AS DATE), 'yyyy-MM-dd'))) SELECT year_id, quarter, total_sales FROM sales_by_quarter WHERE total_sales = (SELECT MIN(total_sales) FROM sales_by_quarter);


e. In which country sales was maximum and in which country sales was minimum
SELECT country, SUM(sales) AS total_sales FROM sales_orders GROUP BY country ORDER BY total_sales DESC LIMIT 1;
SELECT country, SUM(sales) AS total_sales FROM sales_orders GROUP BY country ORDER BY total_sales ASC LIMIT 1;



f. Calculate quartelry sales for each city
select city, quarter(DATE_FORMAT(CAST(CONCAT(year_id, '-', month_id, '-01') AS DATE), 'yyyy-MM-dd')) as quarter, sum(sales) as quarterly_sales from sales_orders group by city, quarter(DATE_FORMAT(CAST(CONCAT(year_id, '-', month_id, '-01') AS DATE), 'yyyy-MM-dd')) order by city, quarter;




h. Find a month for each year in which maximum number of quantities were sold
select month_id, year_id, max_quantity_sold from (select month_id, year_id, sum(quantityordered) as total_quantity_sold, row_number() over (partition by year_id order by sum(quantityordered) desc) as rn from sales_orders group by month_id, year_id) ranked_sales where rn = 1;


