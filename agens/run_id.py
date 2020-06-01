import random
import sys
import os
import datetime
from timeit import default_timer as timer

from query_runner_new import *
    
def run_i_delete(name_data, param_path,i):
    #create result folder
    if not os.path.exists(os.path.dirname("./result/")):
        try:
            os.makedirs(os.path.dirname("./result/"))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    runner = AgensQueryRunner();

    ofile = open("result/i_delete" + "_" + str(i)+"_" + name_data, 'a')
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
            start = timer()
            runner.i_delete_1(params)
            end = timer()
        elif i == 2:
            start = timer()
            runner.i_delete_2(params)
            end = timer()
        elif i == 3:
            start = timer()
            runner.i_delete_3(params)
            end = timer()
        elif i == 4:
            start = timer()
            runner.i_delete_4(params)
            end = timer()
        elif i == 5:
            start = timer()
            runner.i_delete_5(params)
            end = timer()
        elif i == 6:
            start = timer()
            runner.i_delete_6(params)
            end = timer()
        elif i == 7:
            start = timer()
            runner.i_delete_7(params)
            end = timer()
        elif i == 8:
            start = timer()
            runner.i_delete_8(params)
            end = timer()
        exe_time = end - start
        if j != 0:
            total_time += exe_time
        #param = '|'.join(params)
        line = str(n)+": " +name_data + ", " +  "i_delete_"+str(i)+", " + params[0] + ", " + str(exe_time) + " seconds"
        print(line)
        report += line + "\n"
        if n == 100:
            break;
        n += 1
    report += "summary, " +  "i_delete_"+str(i) + ", " + str(total_time/n) + " seconds\n"
    ofile.write(report)
    ofile.write("\n")
    print (report)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run_id.py data_name param_path query_index")
        sys.exit()
    run_i_delete(os.path.basename(sys.argv[1]), sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else "")
