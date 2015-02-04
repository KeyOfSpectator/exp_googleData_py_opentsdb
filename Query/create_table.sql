CREATE TABLE task_events (
   time VARCHAR(15) NOT NULL,
   missing_info VARCHAR(20),
   jobID VARCHAR(20) NOT NULL,
   task_index INTEGER NOT NULL,
   machineID VARCHAR(15),
   event_type INTEGER NOT NULL,
   user VARCHAR(50),
   scheduling_class INTEGER,
   priority INTEGER NOT NULL,
   CPU_request FLOAT,
   memory_request FLOAT,
   disk_space_request FLOAT,
   different_machines_restriction BOOLEAN,
   PRIMARY KEY(time, jobID, task_index, event_type)
);

CREATE TABLE task_events_9 (
   time VARCHAR(15) NOT NULL,
   missing_info VARCHAR(20),
   jobID VARCHAR(20) NOT NULL,
   task_index INTEGER NOT NULL,
   machineID VARCHAR(15),
   event_type INTEGER NOT NULL,
   user VARCHAR(50),
   scheduling_class INTEGER,
   priority INTEGER NOT NULL,
   PRIMARY KEY(time, jobID, task_index, event_type)
);

CREATE TABLE task_events_8 (
   time VARCHAR(15) NOT NULL,
   jobID VARCHAR(20) NOT NULL,
   task_index INTEGER NOT NULL,
   event_type INTEGER NOT NULL,
   CPU_request FLOAT,
   memory_request FLOAT,
   disk_space_request FLOAT,
   different_machines_restriction BOOLEAN,
PRIMARY KEY(time, jobID, task_index, event_type)
);

CREATE TABLE task_events_70 (
   time VARCHAR(15) NOT NULL,
   missing_info VARCHAR(20),
   jobID VARCHAR(20) NOT NULL,
   task_index INTEGER NOT NULL,
   machineID VARCHAR(15),
   event_type INTEGER NOT NULL,
   user VARCHAR(50),  
   PRIMARY KEY(time, jobID, task_index, event_type)
);


CREATE TABLE task_events_71 (
   time VARCHAR(15) NOT NULL,
   jobID VARCHAR(20) NOT NULL,
   task_index INTEGER NOT NULL,
   event_type INTEGER NOT NULL,
   scheduling_class INTEGER,
   priority INTEGER NOT NULL,
   CPU_request FLOAT,
PRIMARY KEY(time, jobID, task_index, event_type)
);

CREATE TABLE task_events_72 (
   time VARCHAR(15) NOT NULL,
   jobID VARCHAR(20) NOT NULL,
   task_index INTEGER NOT NULL,
   event_type INTEGER NOT NULL,
   memory_request FLOAT,
   disk_space_request FLOAT,
   different_machines_restriction BOOLEAN,
PRIMARY KEY(time, jobID, task_index, event_type)
);

CREATE TABLE task_usage (
start_time VARCHAR(15) NOT NULL,
end_time VARCHAR(15) NOT NULL,
jobID VARCHAR(20) NOT NULL,
task_index INTEGER NOT NULL,
machineID VARCHAR(15) NOT NULL,
CPU_rate FLOAT,
canonical_memory_usage FLOAT,
assigned_memory_usage FLOAT,
unmapped_page_cache FLOAT,
total_page_cache FLOAT,
maximum_memory_usage FLOAT,
disk_IO_time FLOAT,
local_disk_space_usage FLOAT,
maximum_CPU_rate FLOAT,
maximum_disk_IO_time FLOAT,
cycles_per_instruction FLOAT,
memory_accesses_per_instruction FLOAT,
sample_portion FLOAT,
aggregation_type BOOLEAN,
sampled_CPU_usage FLOAT,
PRIMARY KEY(start_time, end_time, jobID, task_index)
);


select jobID, task_index, count(*)
from task_usage
group by jobID, task_index
order by count(*) desc
limit 1;

most common jobID, task_index
5285926325          0

select CPU_rate
from task_usage
where jobID=5285926325 and task_index=0 and start_time=3276000000;

select CPU_rate
from task_usage
where jobID=5285926325 and task_index=0 and start_time>=3276000000 and start_time<=3299000000;


select count(*)
from task_events
where event_type=3;
