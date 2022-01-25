Python script by which you can download shared Onedata space with all its content. Also a directory or file can be downloaded.

Requirements:
- Python 3
- module requests

Python 3 can be installed from URL
https://www.python.org/downloads/

Module requests can be installed by commands

```
pip3 install requests
```

Script can be run by commands

```
python3 download.py FILE_ID
```

or

```
./download.py FILE_ID
```

Arguments:
```
positional arguments:
  file_id            File ID of space, directory or file which should be downloaded

optional arguments:
  -h, --help         show this help message and exit
  --onezone ONEZONE  Onezone hostname with protocol (default https://datahub.egi.eu)
```

Examples:
```
./download.py 00000000007E6C76736861726547756964233039383266613462303663623832666666623932633661366363396433636432636837353962233037646231353336326536646363363633393039396136613030383537643738636832366538233134613830313936336235363761656533376665396536633536666434636235636834653138
```

or with onezone specified

```
./download.py --onezone https://datahub.egi.eu  00000000007E6C76736861726547756964233039383266613462303663623832666666623932633661366363396433636432636837353962233037646231353336326536646363363633393039396136613030383537643738636832366538233134613830313936336235363761656533376665396536633536666434636235636834653138
```
