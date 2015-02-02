orders = LOAD 'dualcore/orders'
		AS (
			order_id:int,
			cust_id:int,
			order_dtm:chararray
		);

details = LOAD 'dualcore/order_details'
		AS (
			order_id:int,
			prod_id:int
		);

-- sampling for test
--orders = SAMPLE orders 0.01;
--details = SAMPLE details 0.01;

-- include only the months we want to analyze
recent = FILTER orders BY order_dtm matches '^2013-0[2345]-.*$';

-- include only the product we're advertising
tablets = FILTER details BY prod_id == 1274348;

-- TODO (A): Join the two relations on the order_id field
joined = JOIN recent BY order_id, tablets BY order_id;
--DESCRIBE joined;

-- TODO (B): Create a new relation containing just the month
order_year_month = FOREACH joined GENERATE SUBSTRING(recent::order_dtm, 0, 7) as year_month;
--DESCRIBE order_year_month;

-- TODO (C): Group by month and then count the records in each group
group_order_per_month = GROUP order_year_month BY year_month;
--DESCRIBE group_order_per_month;
order_per_month = FOREACH group_order_per_month GENERATE group, COUNT(order_year_month.year_month);
--DESCRIBE order_per_month;

-- TODO (D): Display the results to the screen
DUMP order_per_month;
