# Cache Simulator(WIP)

A POC simulator for a whole caching stack of three main components:
 filter, caching algorithm and the hardware abstraction that the
 caching algorithm utilizes.

Reports back the object miss rate and byte miss rate.


### How to use


```
python run.py [-h] [--runProfiler] [--writeProfilerResult] [--writeSimResult]
              traceFile cacheSize
```


### Simulation Results
Change the directory path in settings.py to logs the results to a different
directory.

Filenames for the simulation result is generated as the following:
```
{filter name}_{cache name}_{trace filename}_{simulation time}
```
