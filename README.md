Python script by which you can download content of Onedata Share. The script can download whole directory recursively or also a single file.

Requirements:
- Python 3
- Python module `requests`

Python 3 can be installed from URL
https://www.python.org/downloads/

Module requests can be installed by command

```
pip3 install requests
```

Script can be run by commands:

```
python3 download.py FILE_ID
```

or

```
./download.py FILE_ID
```

`FILE_ID` is a string which can be obtained from a web page of the Onedata Share. The `File ID` is listed in `File Details` window accessible by
- click on the icon of the desired node (directory or file), 
- right click on the node and choosing menu item `Information`.

Arguments:
```
positional arguments:
  file_id               File ID of shared space, directory or a file

options:
  -h, --help            show this help message and exit
  -o ONEZONE, --onezone ONEZONE
                        Onedata Onezone URL with specified protocol (default: https://datahub.egi.eu)
  -d DIRECTORY, --directory DIRECTORY
                        Output directory (default: current directory)
  -c CHUNK_SIZE, --chunk-size CHUNK_SIZE
                        The size of downloaded file segments (chunks) after which the file is written to disk. Value can be in bytes, or a number with
                        unit (e.g. 16k, 32M, 2G)
  -j THREADS_NUMBER, --threads-number THREADS_NUMBER
                        Number of threads for parallel downloading. Setting this parameter to a reasonable value can significantly reduce the overall
                        download time.
  -v, --verbose         Set verbose prints - displaying debug information
```

Examples:
```
./download.py 00000000007E6C76736861726547756964233039383266613462303663623832666666623932633661366363396433636432636837353962233037646231353336326536646363363633393039396136613030383537643738636832366538233134613830313936336235363761656533376665396536633536666434636235636834653138
```

or with a Onezone specified

```
./download.py --onezone https://datahub.egi.eu  00000000007E6C76736861726547756964233039383266613462303663623832666666623932633661366363396433636432636837353962233037646231353336326536646363363633393039396136613030383537643738636832366538233134613830313936336235363761656533376665396536633536666434636235636834653138
```

You can run the script directly from the repository by command:
```
curl -s https://raw.githubusercontent.com/CERIT-SC/onedata-downloader/master/download.py | python3 - 00000000007E6C76736861726547756964233039383266613462303663623832666666623932633661366363396433636432636837353962233037646231353336326536646363363633393039396136613030383537643738636832366538233134613830313936336235363761656533376665396536633536666434636235636834653138
```
