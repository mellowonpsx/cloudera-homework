-- Define an alias for the supplied Python script we 
-- use to parse the metadata from the MP3 files
DEFINE tagreader `readtags.py`;

-- load the list of MP3 files to analyze
--calls = LOAD 'test_call_list.txt' AS (file:chararray);
calls = LOAD 'call_list.txt' AS (file:chararray);

-- Use the STREAM keyword to invoke our script, and parse
-- MP3 metadata, returning the fields shown here
metadata = STREAM calls THROUGH tagreader AS (path:chararray,
           category:chararray,
           agent_id:chararray,
           customer_id:chararray,
           timestamp:chararray);


-- TODO (A): Replace the hardcoded '2013-04' with
-- a parameter specified on the command line
-- pig -x local -param MONTH=2013-04 extract_metadata.pig
-- pig -x local -param MONTH=2013-05 extract_metadata.pig
--DUMP metadata;
by_month = FILTER metadata BY SUBSTRING(timestamp, 0, 7) == '$MONTH';
--DUMP by_month;

-- TODO (B): Count calls by category
grouped_by_category = GROUP by_month BY category;
count_by_category = FOREACH grouped_by_category GENERATE group AS category, COUNT(by_month) AS total_num;


-- TODO (C): Display the top three categories to the screen
ordered = ORDER count_by_category BY total_num DESC;
top_3 = LIMIT ordered 3;
DUMP top_3;
