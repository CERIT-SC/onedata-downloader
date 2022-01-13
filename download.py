#!/usr/bin/env python3

"""
Python 3 script which can download Onedata space or a directory recursively with all its content. 

Requirements:
- Python 3
- curl
"""

import argparse
import os
import json
import subprocess
import requests

def download_file(onezone, file_id, file_name, directory):
    """
    Download file with given file_id to given directory.
    """
    # don't download the file when it exists
    if not os.path.exists(directory + os.sep + file_name):
        print("Downloading file", directory + os.sep + file_name)
        url = onezone + "/api/v3/onezone/shares/data/" + file_id + "/content"
        args = ["curl", "--output", directory + os.sep + file_name, "-sL", url]
        process = subprocess.run(args, stdout=subprocess.PIPE)
        if process.returncode == 0:
            print("File", directory + os.sep + file_name, "downloaded")
        else:
            print("Error when downloading file", directory + os.sep + file_name)
    else:
        print("File", directory + os.sep + file_name, "exists, download skipped")

def process_directory(onezone, file_id, file_name, directory):
    """
    Process directory and recursively its content.
    """
    # don't create the the directory when it exists
    if not os.path.exists(directory + os.sep + file_name):
        print("Creating directory", directory + os.sep + file_name)
        os.mkdir(directory + os.sep + file_name)
    else:
        print("Directory", directory + os.sep + file_name, "exists, not created")
    
    # get content of new directory
    url = onezone + "/api/v3/onezone/shares/data/" + file_id + "/children"
    command = "curl -sL " + url
    stream = os.popen(command)
    output = stream.read()
    js = json.loads(output)

    # process child nodes
    for child in js['children']:
        process_node(onezone, child['id'], directory + os.sep + file_name)

def process_node(onezone, file_id, directory):
    """
    Process given node (directory or file).
    """
    # get attributes of node (basic information)
    url = onezone + "/api/v3/onezone/shares/data/" + file_id
    response = requests.get(url)
    if response.ok:
        resnonse_json = response.json()
        node_type = resnonse_json["type"]
        node_name = resnonse_json["name"]

        # check if node is directory or folder
        if node_type == "reg":
            download_file(onezone, file_id, node_name, directory)
        elif node_type == "dir":
            process_directory(onezone, file_id, node_name, directory)
        else:
            print("Error: unknown type of file:", node_type)
    else:
        print("Error: failed to retrieve information about node (Does the node exist?). File ID =")
        print(file_id)

def main():
    parser = argparse.ArgumentParser(description='Download whole space, folder or a file from Onedata')
    parser.add_argument("--onezone", default="https://datahub.egi.eu", type=str, help="Onezone hostname with protocol (default https://datahub.egi.eu)")
    parser.add_argument("file_id", type=str, help="File ID of space, directory or file which should be downloaded")
    args = parser.parse_args()

    process_node(args.onezone, args.file_id, ".")

if __name__ == "__main__":
    main()
