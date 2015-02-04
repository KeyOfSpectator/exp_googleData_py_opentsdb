'''
Created on Jan 4, 2015

@author: keyofspectator

"""
    fix the user name 
    delete the illegal characters in string , illegal characters: +,=
"""
    
'''

import os
import csv

INPUT_PATH = "/home/keyofspectator/ubuntu_exp/throughput/fix_data/"
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])

OUTPUT_PATH = "/home/keyofspectator/ubuntu_exp/throughput/fix_data/output/"
OUTPUT_FILENAME = "fixed_file.csv"


def list_2_str(list_tmp):
    """
    list to string
    
    return string
    """
    tmp = ''.join(list_tmp)
    return tmp


def string_fix(str):
    """
    delete the illegal characters in string , illegal characters: +,=
    """
    list_tmp = list(str)
    i = 0
    while i < len(list_tmp):
        if list_tmp[i] == '+' or list_tmp[i] =='=':
            del list_tmp[i]
            i = i-1
        i = i+1
    return list_2_str(list_tmp) 

def fix_user_name(row):
    """
    fix the user name 
    delete the illegal characters in string , illegal characters: +,=
    """
    row[6] = string_fix(row[6])
    return row

def fixed_execute(input_file , output_file):
    
    reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONE)
    row = reader.next()
    spanwriter = csv.writer(output_file , delimiter=',', quotechar='|' , quoting=csv.QUOTE_MINIMAL)
    
    #    
    
    try:
        while row is not None:
            row = fix_user_name(row)
            spanwriter.writerow(row)
            row = reader.next()
    except StopIteration:
        pass
    input_file.close()
    output_file.close()
    print 'fixed end'
    
    
if __name__ == "__main__":
    
    for fileName in INPUT_FILE_LIST:
        
        input_file = open(INPUT_PATH + fileName, 'r')
        
        output_file = open(OUTPUT_PATH + OUTPUT_FILENAME , 'wb')
        
        
        
        fixed_execute(input_file , output_file)
    
        