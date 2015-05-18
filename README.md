# APM监控数据测试 Opentsdb数据处理模块

<p>Opentsdb 数据处理脚本部分 </p>
<p>详细使用请参考 exp_googleData_shell 项目</p>

* opentsdb thoughput 测opentsdb的写入thoughput

<p>运行脚本：exp_googleData_shell -> exp_opentsdb.sh</p>
<p>相关数据处理脚本: exp_googleData_py_opentsdb -> thoughput_opentsdb_DyTh_import.py</p>
<p>数据集 56M 50WLine 监控数据</p>

* opentsdb query 	测opentsdb的查询时间 （写入数据，通过浏览器OPENTSDB_IP/4242查询  或通过restful接口查询）

	* <b>fix data </b>
	* $python insert_task_event_opentsdb_DyTh_fixdata.py /home/ubuntu/data/query_data/opentsdb/task_event/ 1 399997 >> /home/ubuntu/data/query_data/opentsdb/task_event/task_event_fixed.csv

	* $python insert_task_usage_opentsdb_DyTh_fixdata.py /home/ubuntu/data/query_data/opentsdb/task_usage/ 1 1654293 >> /home/ubuntu/data/query_data/opentsdb/task_usage/task_usage_fixed.csv

	* <b> mkmetric </b>

	* <b>task_event </b>
	* $./tsdb mkmetric google.data.event.cpu google.data.event.memory google.data.event.disk

	* <b>task_usage </b>
	* $./tsdb mkmetric google.data.usage.CPU_rate google.data.usage.canonical_memory_usage google.data.usage.assigned_memory_usage google.data.usage.unmapped_page_cache google.data.usage.total_page_cache  google.data.usage.maximum_memory_usage google.data.usage.disk_IO_time google.data.usage.local_disk_space_usage google.data.usage.maximum_CPU_rate google.data.usage.maximum_disk_IO_time google.data.usage.cycles_per_instruction google.data.usage.memory_accesses_per_instruction google.data.usage.sample_portion google.data.usage.aggregation_type google.data.usage.sampled_CPU_usage

	* <b>gzip</b>
	* <b> 然后把 0.csv 打成 .gz 压缩包 0.csv.gz </b>
	* <b> gzip XXX.csv </b>

	* <b> import </b>
 	* $./tsdb import /home/ubuntu/data/query_data/opentsdb/task_event/gz/0.csv.gz

 	* $./tsdb import /home/ubuntu/data/query_data/opentsdb/task_usage/gz/0.csv.gz