#!/usr/bin/env python3

"""
Python script by which you can download Onedata space or directory with all its content. Downloaded directories and files have to be shared in Onedata.

"""

import argparse
import os
try:
    import requests
except:
    print("ModuleNotFoundError: No module named 'requests' (module 'requests' is not installed)")
    print("You can try install it by command:")
    print("pip install requests")
    print("or you can you can follow the steps described on https://cryo-em-docs.readthedocs.io/en/latest/user/download_all.html")
    exit()

"""
Verbosity level.
"""
VERBOSITY = 0

def download_file(onezone, file_id, file_name, directory):
    """
    Download file with given file_id to given directory.
    """
    # don't download the file when it exists
    print("Downloading file", directory + os.sep + file_name, end="... ")
    if not os.path.exists(directory + os.sep + file_name):
        url = onezone + "/api/v3/onezone/shares/data/" + file_id + "/content"
        response = requests.get(url, allow_redirects=True)
        if response.ok:
            try:
                with open(directory + os.sep + file_name, 'wb') as file:
                    file.write(response.content)
                    print("ok")
            except EnvironmentError as e:
                print("failed, exception occured:", e.__class__.__name__)
                if VERBOSITY > 0:
                    print(str(e))
        else:
            print("failed, HTTP response status code =", response.status_code)
            if VERBOSITY > 0:
                print(response.json())
    else:
        print("file exists, skipped")

def process_directory(onezone, file_id, file_name, directory):
    """
    Process directory and recursively its content.
    """
    # don't create the the directory when it exists
    print("Processing directory", directory + os.sep + file_name, end="... ")
    if not os.path.exists(directory + os.sep + file_name):
        print("created")
        os.mkdir(directory + os.sep + file_name)
    else:
        print("directory exists, not created")
    
    # get content of new directory
    url = onezone + "/api/v3/onezone/shares/data/" + file_id + "/children"
    response = requests.get(url)
    if response.ok:
        response_json = response.json()
        
        # process child nodes
        for child in response_json['children']:
            process_node(onezone, child['id'], directory + os.sep + file_name)
    else:
        print("Error: failed to process directory", file_name)
        if VERBOSITY > 0:
            print("processed directory", file_name, " with File ID =", file_id)
            print(response.json())

def process_node(onezone, file_id, directory):
    """
    Process given node (directory or file).
    """
    # get attributes of node (basic information)
    url = onezone + "/api/v3/onezone/shares/data/" + file_id
    response = requests.get(url)
    if response.ok:
        response_json = response.json()
        node_type = response_json["type"]
        node_name = response_json["name"]

        # check if node is directory or folder
        if node_type == "reg":
            download_file(onezone, file_id, node_name, directory)
        elif node_type == "dir":
            process_directory(onezone, file_id, node_name, directory)
        else:
            print("Error: unknown node type")
            if VERBOSITY > 0:
                print("returned node type", node_type, " of node with File ID =", file_id)
    else:
        print("Error: failed to retrieve information about node with File ID =", file_id)
        print("The requested node may not exist.")
        if VERBOSITY > 0:
            print(response.json())

def clean_onezone(onezone):
    """
     Add protocol name if not specified.
    """
    if not onezone.startswith("https://") and not onezone.startswith("http://"):
        onezone = "http://" + onezone
        if VERBOSITY > 0:
            print("Use Onezone", onezone)
    return onezone

def main():
    parser = argparse.ArgumentParser(description='Download whole shared space, directory or a single file from Onedata Oneprovider.')
    parser.add_argument("--onezone", default="https://datahub.egi.eu", type=str, help="Onedata Onezone URL with specified protocol (default: https://datahub.egi.eu)")
    parser.add_argument("-v", "--verbose", action='count', default=0, help="Set verbosity level - displaying debug information (default: 0)")
    parser.add_argument("file_id", type=str, help="File ID of shared space, directory or file")
    args = parser.parse_args()

    # set up verbosity level
    global VERBOSITY
    VERBOSITY = args.verbose

    onezone = clean_onezone(args.onezone)
    process_node(onezone, args.file_id, ".")

if __name__ == "__main__":
    main()
