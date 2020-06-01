import random
import sys
import os
import datetime
from timeit import default_timer as timer

from query_runner_new import *

comma = ','
def to_str(str):
    return "'"+str+"'"
    
def to_str_list(str):
    strs = str.strip().split(';')
    for i in range(len(strs)):
         strs[i] = to_str(strs[i])
    return "[" + comma.join(strs) +"]"
    
def to_int_list(str):
    ints = str.strip().split(';')
    return "["+comma.join(ints)+"]"
    
def to_double_int_list(str):
    dints = str.strip().split(';')
    for i in range(len(dints)):
        dints[i] = "["+dints[i]+"]"
    return "["+comma.join(dints)+"]"
    
def run_i_insert(name_data, param_path,i):
    #create result folder
    if not os.path.exists(os.path.dirname("./result/")):
        try:
            os.makedirs(os.path.dirname("./result/"))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    runner = AgensQueryRunner();

    ofile = open("result/i_insert" + "_" + str(i) +"_" + name_data, 'a')
    report = "\n---------- " + str(datetime.datetime.now()) + "  " + "  ----------\n"
    f_name = "updateStream_0_0_ii{}.csv".format(i)
    params_file = open(os.path.join(param_path, f_name), "r")
    #header = next(params_file)
    params_list=[]
    count = 0
    for number, line in enumerate(params_file):
        p = line.strip().split('|')
        count += 1
        params_list.append(p)
        
    total_time = 0.0
    
    n = 0
    for j in range(len(params_list)):
        params = params_list[j]
        if i == 1:
            params[1] = to_str(params[1])
            params[2] = to_str(params[2])
            params[3] = to_str(params[3])
            params[6] = to_str(params[6])
            params[7] = to_str(params[7])
            params[9] = to_str(params[9]) #languages
            params[10] = to_str(params[10]) #emails
            params[11] = to_int_list(params[11]) #tagIds
            params[12] = to_double_int_list(params[12]) #studyAt
            params[13] = to_double_int_list(params[13]) #workAt
            start = timer()
            runner.i_insert_1(params)
            end = timer()
        elif i == 2:
            start = timer()
            runner.i_insert_2(params)
            end = timer()
        elif i == 3:
            start = timer()
            runner.i_insert_3(params)
            end = timer()
        elif i == 4:
            params[1] = to_str(params[1])
            params[4] = to_int_list(params[4])
            start = timer()
            runner.i_insert_4(params)
            end = timer()
        elif i == 5:
            start = timer()
            runner.i_insert_5(params)
            end = timer()
        elif i == 6:
            if params[1] == '':
                params[1] = 'null'
            else:
                params[1] = to_str(params[1]) #imageFile
            params[3] = to_str(params[3]) #locationIP
            params[4] = to_str(params[4]) #browserUsed
            params[5] = to_str(params[5]) #language
            if params[6] == '':
                params[6] = 'null'
            else:
                params[6] = to_str(params[6]) #content
            params[11] = to_int_list(params[11]) #tagIds
            start = timer()
            runner.i_insert_6(params)
            end = timer()
        elif i == 7:
            params[2] = to_str(params[2]) #locationIP
            params[3] = to_str(params[3]) #browserUsed
            params[4] = to_str(params[4]) #content
            params[10] = to_int_list(params[10]) #tagIds
            start = timer()
            runner.i_insert_7(params)
            end = timer()
        elif i == 8:
            start = timer()
            runner.i_insert_8(params)
            end = timer()
        exe_time = end - start
        if j != 0:
            total_time += exe_time
        #param = '|'.join(params)
        line = str(n)+": " +name_data + ", " +  "i_insert_"+str(i)+", " + params[0] + ", " + str(exe_time) + " seconds"
        print(line)
        report += line + "\n"
        if n == 100:
            break;
        n += 1
    report += "summary, " +  "i_insert_"+str(i)+ ", " + str(total_time/n) + " seconds\n"
    ofile.write(report)
    ofile.write("\n")
    print (report)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run_ii.py data_name param_path query_index")
        sys.exit()
    run_i_insert(os.path.basename(sys.argv[1]), sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else "")
