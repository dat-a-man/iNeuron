### 1. Create table and skip the header row. 

This is how the sales order dataa table is created to store the csv, and the header row is skipped. 

``` 
create table sales_order_data_csv
        (
        ORDERNUMBER int,
        QUANTITYORDERED int,
        PRICEEACH float,
        ORDERLINENUMBER int,
        SALES float,
        STATUS string,
        QTR_ID int,
        MONTH_ID int,
        YEAR_ID int,
        PRODUCTLINE string,
        MSRP int,
        PRODUCTCODE string,
        PHONE string,
        CITY string,
        STATE string,
        POSTALCODE string,
        COUNTRY string,
        TERRITORY string,
        CONTACTLASTNAME string,
        CONTACTFIRSTNAME string,
        DEALSIZE string
        )
        row format delimited
        fields terminated by ','
        tblproperties("skip.header.line.count"="1")
        ; 
```
### 2. The data is stored in the hdfs location using the command:

```
    load data local inpath 'file:///tmp/hive_class/sales_order_data.csv' into table sales_order_data_csv;
```   

### 3. The following commands are used to make the orc tables

``` 
create table sales_order_orc
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
stored as orc;
```

### 4. Load data from "sales_order_data_csv" into "sales_order_orc"
```
from sales_order_data_csv insert overwrite table sales_order_orc select *;
```

## Perform below menioned queries on "sales_order_orc" table :

### a. Calculate total sales per year

```
Select YEAR_ID, Sum(SALES) as Total_Sales from sales_order_orc group by YEAR_ID order by YEAR_ID ASC Limit 3;
```

Answer :\
2003    3516979.547241211   
2004    4724162.593383789 \
2005    1791486.7086791992

### b. Find a product for which maximum orders were placed
 HIVE command used 
 ``` 
 Select PRODUCTLINE, sum(QUANTITYORDERED) as quantity_ordered from sales_order_orc GROUP BY PRODUCTLINE ORDER BY quantity_ordered DESC LIMIT 5;
 ```

Classic Cars were ordered the most and these are the top 5 results in decreasing order of quantity.

Classic Cars    33992 \
Vintage Cars    21069 \
Motorcycles     11663 \
Trucks and Buses        10777 \
Planes  10727

### c. Calculate the total sales for each quarter

```
SELECT QTR_ID, SUM(SALES) FROM sales_order_orc GROUP BY QTR_ID;
```

Answer:\
1         2350817.726501465\
2         2048120.3029174805\
3         1758910.808959961\
4         3874780.010925293


### d. In which quarter sales was minimum

```
SELECT QTR_ID, SUM(SALES) as sales FROM sales_order_orc GROUP BY QTR_ID ORDER BY sales ASC;
```

Answer: From below it can be seen that minimum sales are in 3rd quarter\
3           1758910.808959961\
2            2048120.3029174805\
1            2350817.726501465\
4            3874780.010925293\

### e. In which country sales was maximum and in which country sales was minimum

```
SELECT COUNTRY, SUM(SALES) as sales FROM sales_order_orc GROUP BY COUNTRY ORDER BY sales ASC;
```

Answer: From the below table it can be seen that sales are minimum in Ireland  and maximum in USA.\
Ireland              57756.43029785156\
Philippines         94015.73046875\
Belgium            108412.61962890625\
Switzerland        117713.55859375\
Japan                  188167.81060791016\
Austria                 202062.53033447266\
Sweden             210014.21020507812\
Germany          220472.0897216797\
Canada              224078.55993652344\
Denmark           245637.15063476562\
Singapore           288488.4102783203\
Norway             307463.69970703125\
Finland             329581.91033935547\
Italy                     374674.3109741211\
UK                        478880.45892333984\
Australia              630623.0987548828\
France              1110916.5217895508\
Spain                1215686.9223632812\
USA                   3627982.825744629\

### f. Calculate quartelry sales for each city

```
SELECT CITY, QTR_ID, SUM(SALES) as sales FROM sales_order_orc GROUP BY CITY, QTR_ID;

```

Answer: Only firt few cities are shown

Aaarhus 4       100595.5498046875\
Allentown       2       6166.7998046875\
Allentown       3       71930.61041259766\
Allentown       4       44040.729736328125\
Barcelona       2       4219.2001953125\
Barcelona       4       74192.66003417969\
Bergamo 1       56181.320068359375\
Bergamo 4       81774.40008544922\
Bergen  3       16363.099975585938\
Bergen  4       95277.17993164062\
Boras   1       31606.72021484375\
Boras   3       53941.68981933594\
Boras   4       48710.92053222656\
Boston  2       74994.240234375\
Boston  3       15344.640014648438\
Boston  4       63730.7802734375\


### g. Find a month for each year in which maximum number of quantities were sold

1. Create a new table named table 1 and load the relevant data into it as shown.
```
create table table_1
(
    year int,
    month int,
    quantity_sum int
) 
stored as orc;

from sales_order_orc insert overwrite table table_1 select YEAR_ID, MONTH_ID, sum(QUANTITYORDERED) GROUP BY YEAR_ID, MONTH_ID;
```

2. Then create a new table and store the maximum quantity sold for a month per year in that as shown below.
```
create table table_2
(
    year int,
    quantity int
) 
stored as orc;

from table_1 insert overwrite table table_2 select year, max(quantity_sum) GROUP BY year;
```

3. Join the tables so as to get the answer

```
SELECT year, month, quantity_sum FROM table_1 u INNER JOIN table_2 l ON u.quantity_sum = l.quantity;

 SELECT u.year, u.month, u.quantity_sum FROM table_1 u INNER JOIN table_2 l ON u.quantity_sum = l.quantity ORDER BY u.year;

```

Answer:

Total MapReduce CPU Time Spent: 6 seconds 490 msec\
OK\
2003       11         10179\
2004       11         10678\
2005       5           4357


