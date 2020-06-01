import random
import sys
import os
import datetime
from timeit import default_timer as timer

from query_runner_new import *
    
def run_i_short(name_data, param_path,i):
    #create result folder
    if not os.path.exists(os.path.dirname("./result/")):
        try:
            os.makedirs(os.path.dirname("./result/"))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    runner = Neo4jQueryRunner();

    ofile = open("result/i_short" + "_" + str(i) +"_" + name_data, 'a')
    report = "\n---------- " + str(datetime.datetime.now()) + "  " + "  ----------\n"
    '''
    # generate is parameters
    file_names = []
    if i == 1:
      file_names.append("updateStream_0_0_ii1.csv")
    elif i == 2 or i == 4 or i == 5:
      file_names.append("updateStream_0_0_ii6.csv")
      file_names.append("updateStream_0_0_ii7.csv")
    elif i == 3:
      file_names.append("updateStream_0_0_ii8.csv")
    elif i == 6 or i==7:
      file_names.append("updateStream_0_0_ii7.csv")
    
    params_list=[]
    for f_name in file_names:
        params_file = open(os.path.join(param_path, f_name), "r")
        #header = next(params_file)
        for number, line in enumerate(params_file):
            p = line.strip().split('|')
            if i == 1 or i == 3 or i==4 or i==5: 
                params_list.append(p[0])
            elif i == 2:
                if f_name == "updateStream_0_0_ii6.csv":
                    params_list.append(p[8])
                elif f_name == "updateStream_0_0_ii7.csv":
                    params_list.append(p[6])
            elif i == 6 or i ==7:
                if p[8] != "-1":
                    params_list.append(p[8])
                else:
                    params_list.append(p[9])
    random.shuffle(params_list)
    params_list = params_list[:101]
    '''
    f_name = "interactive_0_0_is{}.csv".format(i)
    params_file = open(os.path.join(param_path, f_name), "r")
    params_list=[]
    for number, line in enumerate(params_file):
        p = line.strip()
        params_list.append(p)
        
    total_time = 0.0
    n = 0
    for j in range(len(params_list)):
        params = params_list[j]
        if i == 1:
            start = timer()
            runner.i_short_1(params)
            end = timer()
        elif i == 2:
            start = timer()
            runner.i_short_2(params)
            end = timer()
        elif i == 3:
            start = timer()
            runner.i_short_3(params)
            end = timer()
        elif i == 4:
            start = timer()
            runner.i_short_4(params)
            end = timer()
        elif i == 5:
            start = timer()
            runner.i_short_5(params)
            end = timer()
        elif i == 6:
            start = timer()
            runner.i_short_6(params)
            end = timer()
        elif i == 7:
            start = timer()
            runner.i_short_7(params)
            end = timer()
        elif i == 8:
            start = timer()
            runner.i_short_8(params)
            end = timer()
        exe_time = end - start
        if j != 0:
            total_time += exe_time

        line = str(n)+": " +name_data + ", " +  "i_short_"+str(i)+", " + params + ", " + str(exe_time) + " seconds"
        print(line)
        report += line + "\n"
        if n == 100:
            break;
        n += 1
    report += "summary, " +  "i_short_"+str(i)+ ", " + str(total_time/n) + " seconds\n"
    ofile.write(report)
    ofile.write("\n")
    print (report)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run_is.py data_name param_path query_index")
        sys.exit()
    run_i_short(os.path.basename(sys.argv[1]), sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else "")
