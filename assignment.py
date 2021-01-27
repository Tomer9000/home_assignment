import argparse
import os
import hashlib
import csv
import multiprocessing
import getpass

parser = argparse.ArgumentParser(description='This script lists all the files in a directory recursively.')
parser.add_argument('dir_name', help='Input directory name')
arg = parser.parse_args()

'''
Global variables -
CSV_NAME: csv file name.
CSV_FIELDNAMES: csv headers.
CPU_COUNT = number of cores, in order to optimise multiprocessing module.
fs_block_size = File system block size, optimze reading files for md5. 
                Default: None, calculated after cheking that the directory exists.
dict_files = Dictionary which can be used by multiprocessing.
'''
CSV_NAME = 'files_with_md5.csv'
CSV_FIELDNAMES = ['number', 'file-path', 'md5-of-the-file']
CPU_COUNT = multiprocessing.cpu_count()
fs_block_size = None
dict_files = multiprocessing.Manager().dict()

def dir_exist(directory):
    '''
    This function check if the directory-argument exists.
    :param directory: the argument dir_name.
    :return: True if the directory is exists, else - Flase.
    '''
    return os.path.exists(f'{directory}') and os.path.isdir(f'{directory}')

def calc_md5(file):
    '''
    This function calcs the md5 (hash) of the each file,
    and saves the result in the global var - dict_files.
    For each key (file) it saved it's md5 hexdigest.
    '''
    try:
        with open(file, "rb") as f:
            file_hash = hashlib.md5()
            chunk = f.read(fs_block_size)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(fs_block_size)
        dict_files[file] = file_hash.hexdigest()
    # If there is no read permission of the file.
    except PermissionError as e:
        print(f"{getpass.getuser()} has no permissions on file: {e.filename}\n"
              f"it's permissions are - \n{os.popen(f'ls -l {e.filename}').read()}")
    # If the file was not found.
    except FileNotFoundError as e:
        print(f"File {e.filename} was not found, probably moved/deleted during the running of the script.")

def create_csv():
    '''
    This functions produces a csv file with the following output
    <number>,<file-path>,<md5-of-the-file>
    '''
    number = 1
    try:
        with open(CSV_NAME, mode='w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            for file_path, md5_file in dict_files.items():
                writer.writerow({'number': number, 'file-path': file_path, 'md5-of-the-file': md5_file})
                number += 1
    # If there are no permissions to create the csv file.
    except PermissionError as e:
        print(f"{getpass.getuser()} has no permissions to create csv file: {e.filename}")

def main_func(direcory):
    '''
    This function lists all the files in a directory recursively,
    and uses other functions to produce a csv file with the necessary information.
    :param direcory: the argument dir_name.
    '''
    if dir_exist(direcory):
        fs_block_size = (os.statvfs(direcory).f_bsize)
        create_csv()
        for root, _, files in os.walk(direcory):
            for file_name in files:
                dict_files[os.path.join(root, file_name)] = None
        # This is used in order to multi task md5 calculation.
        p = multiprocessing.Pool(CPU_COUNT)
        p.map(calc_md5, dict_files.keys())
        create_csv()
    else:
        print(f"The directory \'{direcory}\' does not exist")

main_func(arg.dir_name)
