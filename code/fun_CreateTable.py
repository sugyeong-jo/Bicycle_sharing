# %%
import csv
import ast
import pandas as pd
import time
from itertools import *



# data type 감지 함수
def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
    if type(t) in [int, float]:
        if (type(t) in [int]) and current_type not in ['float', 'varchar']:
            # Use smallest possible int type
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                return 'smallint'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                return 'int'
            else:
                return 'bigint'
        if type(t) is float and current_type not in ['varchar']:
            return 'decimal'
    else:
        return 'varchar'


# create Table sql 생성 함수
def createTable_stat(path, tablename):
    f = open(path, 'r')
    reader = csv.reader(f)
    longest, headers, type_list = [], [], []
    print('--------The data is followed!!--------')
    for row in islice(reader, 0, 5) :
        print(row)
        if len(headers) == 0:
            headers = row
            for col in row:
                longest.append(0)
                type_list.append('')
        else:
            for i in range(len(row)):
                # NA is the csv null value
                if type_list[i] == 'varchar' or row[i] == 'NA':
                    pass
                else:
                    var_type = dataType(row[i], type_list[i])
                    type_list[i] = var_type
            if len(row[i]) > longest[i]:
                longest[i] = len(row[i])
    f.close()
    print('------------------------------------')        

    statement = 'create table '+tablename+' ('

    for i in range(len(headers)):
        if type_list[i] == 'varchar':
            statement = (statement + '\n{} varchar(30),').format(headers[i].lower())
        else:
            statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])

    statement = statement[:-1] + ');'
    print('--------The sql statement is followed!!!!!--------')
    print(statement)
    print('---------------------------------------------')
    return statement
 