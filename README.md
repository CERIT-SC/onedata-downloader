Python script to download the content of Onedata Share (https://onedata.org/#/home/documentation/20.02/doc/using_onedata/shares.html). 
The script allows you to recursively download an entire directory structure or a single file. 

Requirements:
- Python 3
- Python module `requests`

Python 3 can be installed from URL
https://www.python.org/downloads/

Python module requests can be installed like this:

```
pip3 install requests
```

Script can be run by commands:

```
python3 download.py FILE_ID
```

or

```
# In this case, the file must have the executable permission set.
./download.py FILE_ID
```

`FILE_ID` is a string which can be obtained from a web page of the Onedata Share. The `File ID` is listed in `Info` tab accessible by:

- click on the icon of a desired directory or a file, 
- right click on the node and choosing menu item `Information`, 
- you can find the `File ID` value in the `Info` tab. This value can be copied by clicking on the icon in the bottom right corner of the field.

Arguments:
```
positional arguments:
  file_id               Public File ID of shared space, directory or a file

options:
  -h, --help            show this help message and exit
  -o ONEZONE, --onezone ONEZONE
                        Onedata Onezone URL with specified protocol (default: https://datahub.egi.eu)
  -d DIRECTORY, --directory DIRECTORY
                        Output directory (default: current directory)
  -c CHUNK_SIZE, --chunk-size CHUNK_SIZE
                        The size of downloaded file segments (chunks) after which the file is written to disk. Value can be in bytes, or a number with unit
                        e.g. 16k, 32M or 2G (default: 32M).
  -j THREADS_NUMBER, --threads-number THREADS_NUMBER
                        Number of threads for parallel downloading. Setting this parameter to a reasonable value can significantly reduce the overall
                        download time (default: 1).
  -v, --verbose         Set verbose (debug) prints. There are two levels of these prints.
```

Examples:
```
# download the script
wget https://raw.githubusercontent.com/CERIT-SC/onedata-downloader/master/download.py
# or
curl -O https://raw.githubusercontent.com/CERIT-SC/onedata-downloader/master/download.py

# run the script
python3 download.py 00000000007E6C76736861726547756964233039383266613462303663623832666666623932633661366363396433636432636837353962233037646231353336326536646363363633393039396136613030383537643738636832366538233134613830313936336235363761656533376665396536633536666434636235636834653138
```

or with a Onezone hostname specified

```
./download.py --onezone https://datahub.egi.eu  00000000007E6C76736861726547756964233039383266613462303663623832666666623932633661366363396433636432636837353962233037646231353336326536646363363633393039396136613030383537643738636832366538233134613830313936336235363761656533376665396536633536666434636235636834653138
```

You can run the script directly from the repository by command:
```
curl -s https://raw.githubusercontent.com/CERIT-SC/onedata-downloader/master/download.py | python3 - 00000000007E6C76736861726547756964233039383266613462303663623832666666623932633661366363396433636432636837353962233037646231353336326536646363363633393039396136613030383537643738636832366538233134613830313936336235363761656533376665396536633536666434636235636834653138
```
