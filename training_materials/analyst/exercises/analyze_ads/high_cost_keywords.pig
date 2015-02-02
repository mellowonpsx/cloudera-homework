-- TODO (A): Replace 'FIXME' to load the test_ad_data.txt file.
--data = LOAD 'FIXME'
--data = LOAD 'test_ad_data.txt'
data = LOAD 'dualcore/ad_data[1-2]/part*'
			AS (
				campaign_id:chararray,
				date:chararray,
				time:chararray,
				keyword:chararray,
				display_site:chararray,
				placement:chararray,
				was_clicked:int,
				cpc:int
			);

-- TODO (B): Include only records where was_clicked has a value of 1
clicked_once = FILTER data BY was_clicked == 1;

-- TODO (C): Group the data by the appropriate field
grouped_display_site = GROUP clicked_once BY keyword;

/* TODO (D): Create a new relation which includes only the 
 *           display site and the total cost of all clicks 
 *           on that site
 */
total_cost_grouped = FOREACH grouped_display_site GENERATE group AS keyword, SUM(clicked_once.cpc) AS total_cost;
--DUMP total_cost_grouped;

-- TODO (E): Sort that new relation by cost (ascending)
total_cost_grouped = ORDER total_cost_grouped BY total_cost DESC; 

-- TODO (F): Display just the first three records to the screen
top_5 = LIMIT total_cost_grouped 5;
DUMP top_5;
