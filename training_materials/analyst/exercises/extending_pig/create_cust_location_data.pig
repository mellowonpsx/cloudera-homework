orders = LOAD 'dualcore/orders' AS (order_id:int,
             cust_id:int,
             order_dtm:chararray);

customers = LOAD 'dualcore/customers' AS (cust_id:int,
             fname:chararray,
             lname:chararray,
             address:chararray,
             city:chararray,
             state:chararray,
             zipcode:chararray);

latlon = LOAD 'dualcore/distribution/latlon.tsv' AS (zip:chararray,
              lat:double,
              lon:double);


-- Include only records from during the ad campaign
recent_orders = FILTER orders BY SUBSTRING(order_dtm, 0, 7) == '2013-05';

-- Exclude customers who live within two-day delivery area
-- of our current warehouse. We also exclude customers in 
-- Alaska and Hawaii because these are transported by air.
distant_customers = FILTER customers BY (state != 'CA'
                       AND state != 'OR'
                       AND state != 'WA'
                       AND state != 'NV'
                       AND state != 'UT'
                       AND state != 'ID'
                       AND state != 'MT'
                       AND state != 'AZ'
                       AND state != 'NM'
                       AND state != 'CO'
                       AND state != 'WY'
                       AND state != 'AK'
                       AND state != 'HI');

-- Join orders with customer data so we can extract the ZIP code.
cust_orders = JOIN recent_orders BY cust_id, distant_customers BY cust_id;

-- Our goal here is to get one ZIP code per unique customer
id_and_zip = FOREACH cust_orders GENERATE 
                   distant_customers::cust_id AS cust_id, 
                   distant_customers::zipcode AS zip;
uniq_cust = DISTINCT id_and_zip;

-- Join these ZIP codes with latitude and longitude data
joined = JOIN uniq_cust BY zip, latlon BY zip;

-- Store ZIP, latitude, and longitude in HDFS for further analysis
cust_locations = FOREACH joined GENERATE uniq_cust::zip AS zip, lat, lon;

STORE cust_locations INTO 'dualcore/distribution/cust_locations/';
