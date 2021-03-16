-- Load file: Make sure data is under same directory with .pig if using local mode or data is moved to HDFS if using mapreduce mode
inputLines = LOAD 'network_trace' USING PigStorage(' ') AS (Time:chararray, IP:chararray, SourceIP:chararray, remove:chararray, DestIP:chararray, Protocol:chararray, ProtDepData:chararray);

-- Extract useful fields
filteredLine = FILTER inputLines BY Protocol == 'tcp';

formatLine = FOREACH filteredLine GENERATE Time, SUBSTRING(SourceIP, 0, LAST_INDEX_OF(SourceIP, '.')) AS SourceIP, SUBSTRING(DestIP, 0, LAST_INDEX_OF(DestIP, '.')) AS DestIP, Protocol;

-- Group data
SourceIP_group = GROUP formatLine BY SourceIP;

-- Sum Distinct by Source
SourceIP_sum = FOREACH SourceIP_group {
	uniqueDest = DISTINCT formatLine.DestIP;
	GENERATE group, COUNT(uniqueDest) as Dest_Count;
};

-- Order records
orderedIP = ORDER SourceIP_sum BY Dest_Count DESC;
top10IP = LIMIT orderedIP 10;

-- Write to FS
STORE top10IP INTO 'lab4/exp2/output/';
