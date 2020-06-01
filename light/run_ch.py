import random
import sys
import os
import datetime
import pycurl, json
from timeit import default_timer as timer

from lgraph_query_runner import *

format = 'Content-Type:application/json;charset=utf-8'
login_url = "http://localhost:7071/login"
login_usr = json.dumps({"user": "admin", "password":"admin123456"})
login_head = [format]

query_url = "http://127.0.0.1:7071/cypher"

def get_jwt():
    c = pycurl.Curl()
    c.setopt(pycurl.URL, login_url)
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.HTTPHEADER, login_head)
    c.setopt(pycurl.POSTFIELDS, login_usr)
    result = c.perform_rb()
    result = json.loads(result)
    jwt = result["jwt"]
    return jwt

def check(jwt, query):
    auth = 'Authorization:Bearer '+jwt
    head = [format,auth]
    c = pycurl.Curl()
    c.setopt(pycurl.URL, query_url)
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.HTTPHEADER, head)
    data = json.dumps({"graph":"default", "script":query})
    c.setopt(pycurl.POSTFIELDS, data)
    result = c.perform_rb()
    c.close()
    result = json.loads(result)
    print(result)
    time = result["elapsed"]
    return float(time)
    
def run_i_check(name_data, param_path,i):
    #create result folder
    if not os.path.exists(os.path.dirname("./result/")):
        try:
            os.makedirs(os.path.dirname("./result/"))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    ofile = open("result/i_check" + "_" + str(i)+"_" + name_data, 'a')
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
    jwt = get_jwt()
    for j in range(len(params_list)):
        params = params_list[j]
        if i == 1:
            query = i_check_1(params)
            time = check(jwt, query)
        elif i == 2:
            query = i_check_2(params)
            time = check(jwt, query)
        elif i == 3:
            query = i_check_3(params)
            time = check(jwt, query)
        elif i == 4:
            query = i_check_4(params)
            time = check(jwt, query)
        elif i == 5:
            query = i_check_5(params)
            time = check(jwt, query)
        elif i == 6:
            query = i_check_6(params)
            time = check(jwt, query)
        elif i == 7:
            query = i_check_7(params)
            time = check(jwt, query)
        elif i == 8:
            query = i_check_8(params)
            time = check(jwt, query)
        exe_time = time
        if j != 0:
            total_time += exe_time
        #param = '|'.join(params)
        line = str(n)+": " +name_data + ", " + "i_check_"+str(i)+", " + params[0] + ", " + str(exe_time) + " seconds"
        print(line)
        report += line + "\n"
        if n == 100:
            break;
        n += 1
    report += "summary, " +  "i_check_"+str(i) + ", " + str(total_time/n) + " seconds\n"
    ofile.write(report)
    ofile.write("\n")
    print (report)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run_ch.py data_name param_path query_index")
        sys.exit()
    run_i_check(os.path.basename(sys.argv[1]), sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else "")
