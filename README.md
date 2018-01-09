# mytarget
Collection of classes to work with myTarget API


### Versions

* Update tokens - APIv2
* Update Campaigns - APIv1
* Get Stats - APIv2 / APIv2

### ?

```
# python3 mt_interface.py -h
usage: mt_interface.py [-h] [-l] [-t] [-cl [CLIENTS_LIST [CLIENTS_LIST ...]]]
                  [-dr [DATE_RANGE [DATE_RANGE ...]]]
                  {clients,campaigns,counters,stats,stats_v2}

Interface for myTarget API

positional arguments:
  {clients,campaigns,counters,stats,stats_v2}
                        complete task

optional arguments:
  -h, --help            show this help message and exit
  -l, --show_log        print execution log
  -t, --with_threading  perform task in parallel mode
  -cl [CLIENTS_LIST [CLIENTS_LIST ...]], --clients_list [CLIENTS_LIST [CLIENTS_LIST ...]]
                        produce clients list
  -dr [DATE_RANGE [DATE_RANGE ...]], --date_range [DATE_RANGE [DATE_RANGE ...]]
                        produce date range
```

### Example

Run from command line

```bash
python3 interface.py stats -l \
        --clients_list 001 002 003 \
        --date_range 2017-12-12 2017-12-13 >> myapitarget_exec_log.json 2>&1
```

#### Requirements

* Python 3.0 or higher
