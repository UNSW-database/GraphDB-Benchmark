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
    
def add_date(st, dur):
    start_time = datetime.datetime.strptime(st,'%Y-%m-%d %H:%M:%S')
    end_time = start_time + datetime.timedelta(days=int(dur))
    et = end_time.strftime('%Y-%m-%d %H:%M:%S')
    return change_date_format(et)
    
def run_i_complex(name_data, param_path,i):
    #create result folder
    if not os.path.exists(os.path.dirname("./result/")):
        try:
            os.makedirs(os.path.dirname("./result/"))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    runner = AgensQueryRunner();
    
    ofile = open("result/i_complex" +"_" + str(i) +"_" + name_data, 'a')
    report = "\n---------- " + str(datetime.datetime.now()) + "  " + "  ----------\n"
    f_name = "IC_{}.txt".format(i)
    params_file = open(os.path.join(param_path, f_name), "r")
    header = next(params_file)
    params =[]
    for number, line in enumerate(params_file):
        params = line.strip().split('|')
        if i == 1 or i == 6 or i == 11 or i == 12:
            params[1] = to_str(params[1]) #firstName
        elif i == 2 or i == 5 or i == 9:
            params[1] = change_date_format(params[1]) #date
        elif i == 3:
            params[2] = add_date(params[1], params[2]) #endDate
            params[1] = change_date_format(params[1]) #startDate
            params[3] = to_str(params[3]) #countryX
            params[4] = to_str(params[4]) #countryY
        elif i == 4:
            params[2] = add_date(params[1], params[2]) #endDate
            params[1] = change_date_format(params[1]) #startDate
            
    total_time = 0.0
    for j in range(0, 3):
        if i == 1:
            start = timer()
            runner.i_complex_1(params)
            end = timer()
        elif i == 2:
            start = timer()
            runner.i_complex_2(params)
            end = timer()
        elif i == 3:
            start = timer()
            runner.i_complex_3(params)
            end = timer()
        elif i == 4:
            start = timer()
            runner.i_complex_4(params)
            end = timer()
        elif i == 5:
            start = timer()
            runner.i_complex_5(params)
            end = timer()
        elif i == 6:
            start = timer()
            runner.i_complex_6(params)
            end = timer()
        elif i == 7:
            start = timer()
            runner.i_complex_7(params)
            end = timer()
        elif i == 8:
            start = timer()
            runner.i_complex_8(params)
            end = timer()
        elif i == 9:
            start = timer()
            runner.i_complex_9(params)
            end = timer()
        elif i == 10:
            start = timer()
            runner.i_complex_10(params)
            end = timer()
        elif i == 11:
            start = timer()
            runner.i_complex_11(params)
            end = timer()
        elif i == 12:
            start = timer()
            runner.i_complex_12(params)
            end = timer()
        elif i == 13:
            start = timer()
            runner.i_complex_13(params)
            end = timer()
        elif i == 14:
            c14_weight =''
            weight_precal = open("weight_precal.sql") # stored procedure
            for l, line in enumerate(weight_precal):
                c14_weight += line;
            weight_precal.close();
            c14_func =''
            proc = open("proc.sql")
            for l, line in enumerate(proc):
                c14_func += line;
            proc.close();
            start = timer()
            runner.i_complex_14(params, c14_weight, c14_func)
            end = timer()
        exe_time = end - start
        if j != 0:
            total_time += exe_time
        param = '|'.join(params)
        line = str(j)+": " +name_data + ", " +  "i_complex_"+str(i)+", " + param + ", " + str(exe_time) + " seconds"
        print(line)
        report += line + "\n"
    if j != 0:
        report += "summary, " +  "i_complex_"+str(i) + ", " + str(total_time/j) + " seconds\n"
    ofile.write(report)
    ofile.write("\n")
    print (report)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run_ic.py data_name param_path query_index")
        sys.exit()
    run_i_complex(os.path.basename(sys.argv[1]), sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else "")
