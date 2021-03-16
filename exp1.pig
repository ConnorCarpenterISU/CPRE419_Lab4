-- Load file: Make sure data is under same directory with .pig if using local mode or data is moved to HDFS if using mapreduce mode
inputLines = LOAD 'network_trace' USING PigStorage(' ') AS (Time:chararray, IP:chararray, SourceIP:chararray, remove:chararray, DestIP:chararray, Protocol:chararray, ProtDepData:chararray);

-- Extract useful fields
extractLine = FOREACH inputLines GENERATE USPS, ALAND;

-- Group data
land_group = GROUP extractLine BY USPS;

-- Sum by state
land_sum = FOREACH land_group GENERATE SUM(extractLine.ALAND) AS ALAND_TOTAL, group AS USPS;

-- Order records
orderedLand = ORDER land_sum BY ALAND_TOTAL DESC;
top10Land = LIMIT orderedLand 10;

-- Write to FS
STORE top10Land INTO 'lab4/exp2/';
