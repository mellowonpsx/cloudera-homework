data = LOAD 'dualcore/orders'
	AS (
		order_id:int,
		cust_id:int,
		order_dtm:chararray
	);
--data = SAMPLE data 0.001;
/*
 * Include only records where the 'order_dtm' field matches
 * the regular expression pattern:
 *
 *   ^       = beginning of string
 *   2013    = literal value '2013'
 *   0[2345] = 0 followed by 2, 3, 4, or 5
 *   -       = a literal character '-'
 *   \\d{2}  = exactly two digits
 *   \\s     = a single whitespace character
 *   .*      = any number of any characters
 *   $       = end of string
 *
 * If you are not familiar with regular expressions and would
 * like to know more about them, see the Regular Expression 
 * Reference at the end of the Exercise Manual.
 */
recent = FILTER data by order_dtm matches '^2013-0[2345]-\\d{2}\\s.*$';

-- TODO (A): Create a new relation with just the order's year and month
order_year_month = FOREACH recent GENERATE SUBSTRING(order_dtm, 0, 7) as year_month;

-- TODO (B): Count the number of orders in each month
group_order_per_month = GROUP order_year_month BY year_month;
order_per_month = FOREACH group_order_per_month GENERATE group, COUNT(order_year_month.year_month);

-- TODO (C): Display the count by month to the screen.
DUMP order_per_month;
