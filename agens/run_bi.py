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
    
def change_date_format(str):
    str = str.replace('-', '')
    str = str.replace(' ', '')
    str = str.replace(':', '')
    return str + '000'

def change_birthday_format(str):
    str = str.replace('-', '')
    str = str.replace(' ', '')
    str = str.replace(':', '')
    return str
    
def run_bi(name_data, param_path,i):
    #create result folder
    if not os.path.exists(os.path.dirname("./result/")):
        try:
            os.makedirs(os.path.dirname("./result/"))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    runner = AgensQueryRunner();

    ofile = open("result/business_i" +"_" + str(i) +"_" + name_data, 'a')
    report = "\n---------- " + str(datetime.datetime.now()) + "  " + "  ----------\n"
    f_name = "BI_{}.txt".format(i)
    params_file = open(os.path.join(param_path, f_name), "r")
    header = next(params_file)
    params =[]
    for number, line in enumerate(params_file):
        params = line.strip().split('|')
        if i == 1 or i == 12:
            params[0] = change_date_format(params[0]) #date
        elif i == 2:
            params[0] = change_date_format(params[0]) #startDate
            params[1] = change_date_format(params[1]) #endDate
            params[2] = to_str(params[2]) #country1
            params[3] = to_str(params[3]) #country2
        elif i == 4 or i == 9 or i == 22:
            params[0] = to_str(params[0]) #tagClass
            params[1] = to_str(params[1]) #country
        elif i == 5 or i == 6 or i == 7 or i == 8 or i == 13 or i == 15 or i == 17 or i == 23 or i == 24:
            params[0] = to_str(params[0]) #country or tag
        elif i == 10 or i == 21:
            params[0] = to_str(params[0]) #tag
            params[1] = change_date_format(params[1]) #date
        elif i == 11:
            params[0] = to_str(params[0]) #country
            params[1] = to_str_list(params[1]) #blocklist
        elif i == 14:
            params[0] = change_date_format(params[0]) #startDate
            params[1] = change_date_format(params[1]) #endDate
        elif i == 16:
            params[1] = to_str(params[1]) #country
            params[2] = to_str(params[2]) #tagClass
        elif i == 18:
            params[0] = change_date_format(params[0]) #date
            params[2] = to_str_list(params[2]) #languages
        elif i == 19:
            params[0] = change_birthday_format(params[0]) #date
            params[1] = to_str(params[1]) #tagClass1
            params[2] = to_str(params[2]) #tagClass2
        elif i == 20:
            params[0] = to_str_list(params[0]) #tagClassName
        elif i == 25:
            params[2] = change_date_format(params[2]) #startDate
            params[3] = change_date_format(params[3]) #endDate
            
    total_time = 0.0
    for j in range(0, 3):
        try:
            if i == 1:
                start = timer()
                runner.bi_1(params)
                end = timer()
            elif i == 2:
                start = timer()
                runner.bi_2(params)
                end = timer()
            elif i == 3:
                start = timer()
                runner.bi_3(params)
                end = timer()
            elif i == 4:
                start = timer()
                runner.bi_4(params)
                end = timer()
            elif i == 5:
                start = timer()
                runner.bi_5(params)
                end = timer()
            elif i == 6:
                start = timer()
                runner.bi_6(params)
                end = timer()
            elif i == 7:
                start = timer()
                runner.bi_7(params)
                end = timer()
            elif i == 8:
                start = timer()
                runner.bi_8(params)
                end = timer()
            elif i == 9:
                start = timer()
                runner.bi_9(params)
                end = timer()
            elif i == 10:
                start = timer()
                runner.bi_10(params)
                end = timer()
            elif i == 11:
                start = timer()
                runner.bi_11(params)
                end = timer()
            elif i == 12:
                start = timer()
                runner.bi_12(params)
                end = timer()
            elif i == 13:
                start = timer()
                runner.bi_13(params)
                end = timer()
            elif i == 14:
                start = timer()
                runner.bi_14(params)
                end = timer()
            elif i == 15:
                start = timer()
                runner.bi_15(params)
                end = timer()
            elif i == 16:
                start = timer()
                runner.bi_16(params)
                end = timer()
            elif i == 17:
                start = timer()
                runner.bi_17(params)
                end = timer()
            elif i == 18:
                start = timer()
                runner.bi_18(params)
                end = timer()
            elif i == 19:
                start = timer()
                runner.bi_19(params)
                end = timer()
            elif i == 20:
                start = timer()
                runner.bi_20(params)
                end = timer()
            elif i == 21:
                start = timer()
                runner.bi_21(params)
                end = timer()
            elif i == 22:
                start = timer()
                runner.bi_22(params)
                end = timer()
            elif i == 23:
                start = timer()
                runner.bi_23(params)
                end = timer()
            elif i == 24:
                start = timer()
                runner.bi_24(params)
                end = timer()
            elif i == 25:
                start = timer()
                runner.bi_25(params)
                end = timer()
            exe_time = end - start
            if j != 0:
                total_time += exe_time
            param = '|'.join(params)
            line = str(j)+": " +name_data + ", " +  "bi_"+str(i)+", " + param + ", " + str(exe_time) + " seconds"
            print(line)
        except Exception as e:
            line = str(e)
        report += line + "\n"
    if j != 0:
        report += "summary, " +  "bi_"+str(i) + ", " + str(total_time/j) + " seconds\n"
    ofile.write(report)
    ofile.write("\n")
    print (report)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run_bi.py data_name param_path query_index")
        sys.exit()
    run_bi(os.path.basename(sys.argv[1]), sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else "")
