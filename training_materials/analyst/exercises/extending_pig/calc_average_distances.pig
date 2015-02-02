-- Register DataFu and define an alias for the function
REGISTER '/usr/lib/pig/datafu-*.jar';
DEFINE DIST datafu.pig.geo.HaversineDistInMiles;

cust_locations = LOAD 'dualcore/distribution/cust_locations/'
                   AS (zip:chararray,
                       lat:double,
                       lon:double);

warehouses = LOAD 'dualcore/distribution/warehouses.tsv'
                   AS (zip:chararray,
                       lat:double,
                       lon:double);
             


-- Create a record for every combination of customer and
-- proposed distribution center location.
crossed = CROSS cust_locations, warehouses;
--DESCRIBE crossed;

-- Calculate the distance from the customer to the warehouse
distance = FOREACH crossed GENERATE warehouses::zip AS zip, DIST(cust_locations::lat, cust_locations::lon, warehouses::lat, warehouses::lon) AS dist;
--DESCRIBE distance;

-- Calculate the average distance for all customers to each warehouse
grouped_by_zip = GROUP distance BY zip;
--DESCRIBE grouped_by_zip;

avg_distance_by_zip = FOREACH grouped_by_zip GENERATE group AS zip, AVG(distance.dist) AS average;
--DESCRIBE avg_distance_by_zip;

-- Display the result to the screen
DUMP avg_distance_by_zip;
