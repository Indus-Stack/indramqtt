`observability.monitor_sample_secs` sets the interval of the monitor sampler. The sampler copies live counters into the monitor ring at that interval.

0 records one boot sample and stops the periodic tick. The default is 10 seconds. The range is 0 to 3600.
