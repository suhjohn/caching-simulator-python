# Cache Simulator(WIP)

A POC simulator for a whole caching stack of three main components:
 filter, caching algorithm and the hardware abstraction that the
 caching algorithm utilizes.

Reports back the object miss rate and byte miss rate.

To execute:

```
python run.py trace_filepath cache_size
```

Change constants in constants.py for experimenting different values.