--data = LOAD 'sample2.txt' USING PigStorage(',')
data = LOAD 'dualcore/ad_data2.txt' USING PigStorage(',')
			AS (
				campaign_id: chararray,
				date: chararray,
				time: chararray,
				display_site: chararray,
				placement: chararray,
				was_clicked: int,
				cpc: int,
				keyword: chararray
			);
--DESCRIBE data;
unique_data = DISTINCT data;
--DUMP unique_data;
elaborate_data = FOREACH unique_data GENERATE
						campaign_id,
						REPLACE(date, '-', '/') AS date: chararray,
						time,
						UPPER(TRIM(keyword)) AS keyword: chararray,
						display_site,
						placement,
						was_clicked,
						cpc;
--DUMP unique_data;
STORE elaborate_data INTO 'dualcore/ad_data2';
