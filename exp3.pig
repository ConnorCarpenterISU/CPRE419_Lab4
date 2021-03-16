-- Load file: Make sure data is under same directory with .pig if using local mode or data is moved to HDFS if using mapreduce mode
ip_trace = LOAD 'ip_trace' USING PigStorage(' ') AS (Time:chararray, ConnID:chararray, SourceIP:chararray, remove:chararray, DestIP:chararray, Protocol:chararray, ProtDepData:chararray);
raw_block = LOAD 'raw_block' USING PigStorage(' ') AS (ConnectionID:chararray, Action:chararray);

IPTrace_line = FOREACH ip_trace GENERATE Time, ConnID, (ENDSWITH(SourceIP, ':') ? SUBSTRING(SourceIP, 0, LAST_INDEX_OF(SourceIP, ':')) : SourceIP) AS SourceIP, (ENDSWITH(DestIP, ':') ? SUBSTRING(DestIP, 0, LAST_INDEX_OF(DestIP, ':')) : DestIP) AS DestIP, Protocol;
IPTrace_line2 = FOREACH IPTrace_line GENERATE Time, ConnID, (Protocol == 'tcp' ? SUBSTRING(SourceIP, 0, LAST_INDEX_OF(SourceIP, '.')) : SourceIP) AS SourceIP, (Protocol == 'tcp' ? SUBSTRING(DestIP, 0, LAST_INDEX_OF(DestIP, '.')) : DestIP) AS DestIP, Protocol;
IPTrace_line3 = FOREACH IPTrace_line2 GENERATE Time, ConnID, (Protocol == 'UDP,' ? SUBSTRING(SourceIP, 0, LAST_INDEX_OF(SourceIP, '.')) : SourceIP) AS SourceIP, (Protocol == 'UDP,' ? SUBSTRING(DestIP, 0, LAST_INDEX_OF(DestIP, '.')) : DestIP) AS DestIP, Protocol;

RawBlock_line = FOREACH raw_block GENERATE ConnectionID, Action;

join_lines = JOIN IPTrace_line3 BY ConnID, RawBlock_line BY ConnectionID;

block_filter = FILTER join_lines BY Action == 'Blocked';

firewall_log = FOREACH block_filter GENERATE Time, ConnID, SourceIP, DestIP, Action;

-- Write to FS
STORE firewall_log INTO 'lab4/exp3/firewall';

-- Group data
SourceIP_group = GROUP firewall_log BY SourceIP;

blocks = FOREACH SourceIP_group GENERATE COUNT(firewall_log) AS block_count, group;

ordered_blocks = ORDER blocks BY block_count DESC;

STORE ordered_blocks INTO 'lab4/exp3/output';
