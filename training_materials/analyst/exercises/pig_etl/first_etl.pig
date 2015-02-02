--data = LOAD 'sample2.txt' USING PigStorage(',')
data = LOAD 'dualcore/ad_data1.txt' AS (
				keyword: chararray,
				campaign_id: chararray,
				date: chararray,
				time: chararray,
				display_site: chararray,
				was_clicked: int,
				cpc: int,
				country: chararray,
				placement: chararray
				);
usa_only = FILTER data BY NOT (country != 'USA');
-- DUMP usa_only;
-- campaign_id date time keyword display_site placement was_clicked cpc
elaborate_data = FOREACH usa_only GENERATE
						campaign_id,
						date,
						time,
						UPPER(TRIM(keyword)) AS keyword: chararray,
						display_site,
						placement,
						was_clicked,
						cpc;
-- DUMP elaborate_data;
-- STORE elaborate_data INTO 'dualcore/ad_data1' USING JsonStorage();
STORE elaborate_data INTO 'dualcore/ad_data1';
