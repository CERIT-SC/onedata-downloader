#!/usr/bin/env python3

"""
Python script by which you can download Onedata space or directory with all its content. Downloaded directories and files have to be shared in Onedata.

"""

import argparse
import os
import sys
try:
    import requests
except:
    print("ModuleNotFoundError: No module named 'requests' (module 'requests' is not installed)")
    print("You can try install it by command:")
    print("pip install requests")
    print("or you can you can follow the steps described on https://cryo-em-docs.readthedocs.io/en/latest/user/download_all.html")
    sys.exit(1)

"""
Verbosity level.
"""
VERBOSITY = 0

"""
Default Onezone service URL.
"""
DEFAULT_ONEZONE = "https://datahub.egi.eu"

"""
Onezone API address.
"""
ONEZONE_API = "/api/v3/onezone/"

def verbose_print(level, *args, **kwargs):
    """
    Print only when VERBOSITY is equal or higher than given level.
    """
    if VERBOSITY >= level:
        print(*args, **kwargs)

def download_file(onezone, file_id, file_name, directory):
    """
    Download file with given file_id to given directory.
    """
    # don't download the file when it exists
    print("Downloading file", directory + os.sep + file_name, end="... ", flush=True)
    if not os.path.exists(directory + os.sep + file_name):
        url = onezone + ONEZONE_API + "shares/data/" + file_id + "/content"
        response = requests.get(url, allow_redirects=True)
        if response.ok:
            try:
                with open(directory + os.sep + file_name, 'wb') as file:
                    file.write(response.content)
                    print("ok")
                    return 0
            except EnvironmentError as e:
                print("failed, exception occured:", e.__class__.__name__)
                verbose_print(1, str(e))
                return 2
        else:
            print("failed, HTTP response status code =", response.status_code)
            verbose_print(1, response.json())
            return 2
    else:
        print("file exists, skipped")
        return 0

def process_directory(onezone, file_id, file_name, directory):
    """
    Process directory and recursively its content.
    """
    # don't create the the directory when it exists
    print("Processing directory", directory + os.sep + file_name, end="... ", flush=True)
    try:
        os.mkdir(directory + os.sep + file_name, mode=0o777)
        print("directory created")
    except FileExistsError:
        print("directory exists, not created")
    except FileNotFoundError as e:
        print("failed, exception occured:", e.__class__.__name__)
        verbose_print(1, str(e))
        return 2

    # get content of new directory
    url = onezone + ONEZONE_API + "shares/data/" + file_id + "/children"
    response = requests.get(url)
    if response.ok:
        response_json = response.json()

        result = 0
        # process child nodes
        for child in response_json['children']:
            result = process_node(onezone, child['id'], directory + os.sep + file_name) or result

        return result
    else:
        print("Error: failed to process directory", file_name)
        verbose_print(1, "processed directory", file_name, " with File ID =", file_id)
        verbose_print(1, response.json())
        return 2

def process_node(onezone, file_id, directory):
    """
    Process given node (directory or file).
    """
    # get attributes of node (basic information)
    url = onezone + ONEZONE_API + "shares/data/" + file_id
    response = requests.get(url)
    if response.ok:
        response_json = response.json()
        node_type = response_json["type"]
        node_name = response_json["name"]

        result = 0
        # check if node is directory or folder
        if node_type == "reg":
            result = download_file(onezone, file_id, node_name, directory) or result
        elif node_type == "dir":
            result = process_directory(onezone, file_id, node_name, directory) or result
        else:
            print("Error: unknown node type")
            verbose_print(1, "returned node type", node_type, " of node with File ID =", file_id)
            return 2

        return result
    else:
        print("Error: failed to retrieve information about node with File ID =", file_id)
        print("The requested node may not exist.")
        verbose_print(1, response.json())
        return 1

def clean_onezone(onezone):
    """
    Clean and test of given Onezone service.
    """
    # add protocol name if not specified
    if not onezone.startswith("https://") and not onezone.startswith("http://"):
        onezone = "http://" + onezone

    verbose_print(1, "Use Onezone:", onezone)

    # test if such Onezone exists 
    url = onezone + ONEZONE_API + "configuration"
    try:
        response = requests.get(url)
    except Exception as e:
        print("Error: failure while trying to communicate with Onezone:", onezone)
        verbose_print(1, str(e))
        sys.exit(2)

    if not response.ok:
        print("Error: failure while connecting to Onezone:", onezone)
        sys.exit(2)

    try:
        response_json = response.json()
        verbose_print(1, "Onezone configuration:")
        verbose_print(1, response_json)
    except Exception as e:
        print("Error: failure while parsing JSON response from Onezone:", e.__class__.__name__)
        sys.exit(2)

    return onezone

def clean_directory(directory):
    """
    Test if given directory is correct.
    """
    # test if given directory exists
    if not os.path.isdir(directory):
        print("Error: output directory", directory, "does not exist")
        sys.exit(2)

    return directory

def main():
    parser = argparse.ArgumentParser(description='Download whole shared space, directory or a single file from Onedata Oneprovider.')
    parser.add_argument("-o", "--onezone", default=DEFAULT_ONEZONE, type=str, help="Onedata Onezone URL with specified protocol (default: https://datahub.egi.eu)")
    parser.add_argument("-d", "--directory", default=".", type=str, help="Output directory (default: current directory)")
    parser.add_argument("-v", "--verbose", action='count', default=0, help="Set verbose prints - displaying debug information")
    parser.add_argument("file_id", type=str, help="File ID of shared space, directory or a file")
    args = parser.parse_args()

    # set up verbosity level
    global VERBOSITY
    VERBOSITY = args.verbose

    onezone = clean_onezone(args.onezone)
    directory = clean_directory(args.directory)

    try:
        result = process_node(onezone, args.file_id, directory)
        return result
    except KeyboardInterrupt as e:
        print(" prematurely interrupted (" + e.__class__.__name__ + ")")
        return 2

if __name__ == "__main__":
    return_code = main()
    sys.exit(return_code)
