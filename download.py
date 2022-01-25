#!/usr/bin/env python3

"""
Python script by which you can download Onedata space with all its content. 

"""

import argparse
import os
import requests

def download_file(onezone, file_id, file_name, directory):
    """
    Download file with given file_id to given directory.
    """
    # don't download the file when it exists
    print("Downloading file", directory + os.sep + file_name, end="... ")
    if not os.path.exists(directory + os.sep + file_name):
        url = onezone + "/api/v3/onezone/shares/data/" + file_id + "/content"
        request = requests.get(url, allow_redirects=True)
        if request.ok:
            with open(directory + os.sep + file_name, 'wb') as file:
                file.write(request.content)

            print("ok")
        else:
            print("failed")
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
        print("Error: failed to process directory " + file_name + ", file ID =")
        print(file_id)

def process_node(onezone, file_id, directory):
    """
    Process given node (directory or file).
    """
    LENGTH_OF_FILE_ID = 269
    # check file_id
    if len(file_id) == LENGTH_OF_FILE_ID:
        print("Error: it seems that the given file id has incorrect length")
        return

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
