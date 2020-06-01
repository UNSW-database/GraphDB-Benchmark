import random
import sys
import os
import datetime
import pycurl, json
from timeit import default_timer as timer

from lgraph_query_runner import *

comma = ','

format = 'Content-Type:application/json;charset=utf-8'
login_url = "http://127.0.0.1:7071/login"
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

def insert_single(jwt, query):
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
    t = result["elapsed"]
    #header = result["header"]
    #output = result["result"]
    return float(t)

def insert_multi(jwt, queries):
    t = 0.0
    auth = 'Authorization:Bearer '+jwt
    head = [format,auth]
    c = pycurl.Curl()
    c.setopt(pycurl.URL, query_url)
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.HTTPHEADER, head)
    for q in queries:
        data = json.dumps({"graph":"default", "script":q})
        c.setopt(pycurl.POSTFIELDS, data)
        result = c.perform_rb()
        result = json.loads(result)
        print(result)
        elapsed_time = result["elapsed"]
        t += float(elapsed_time)
    c.close()
    return t
    
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
    
def to_single_int_list(str):
    dints = str.strip().split(';')
    for i in range(len(dints)):
        temp = dints[i].strip().split(',')
        dints[i] = temp[0]
    return "["+comma.join(dints)+"]"
    
def run_i_insert(name_data, param_path,i):
    #create result folder
    if not os.path.exists(os.path.dirname("./result/")):
        try:
            os.makedirs(os.path.dirname("./result/"))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

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
    jwt = get_jwt()
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
            params[12] = to_single_int_list(params[12]) #studyAt
            params[13] = to_single_int_list(params[13]) #workAt
            query = i_insert_1(params)
            t = insert_single(jwt,query)
        elif i == 2:
            query = i_insert_2(params)
            t = insert_single(jwt,query)
        elif i == 3:
            query = i_insert_3(params)
            t = insert_single(jwt,query)
        elif i == 4:
            params[1] = to_str(params[1])
            params[4] = to_int_list(params[4])
            query = i_insert_4(params)
            t = insert_single(jwt,query)
        elif i == 5:
            query = i_insert_5(params)
            t = insert_single(jwt,query)
        elif i == 6:
            params[1] = to_str(params[1]) #imageFile
            params[3] = to_str(params[3]) #locationIP
            params[4] = to_str(params[4]) #browserUsed
            params[5] = to_str(params[5]) #language
            params[6] = to_str(params[6]) #content
            params[11] = to_int_list(params[11]) #tagIds
            query = i_insert_6(params)
            t = insert_single(jwt,query)
        elif i == 7:
            params[2] = to_str(params[2]) #locationIP
            params[3] = to_str(params[3]) #browserUsed
            params[4] = to_str(params[4]) #content
            params[10] = to_int_list(params[10]) #tagIds
            query = i_insert_7(params)
            t = insert_single(jwt,query)
        elif i == 8:
            query = i_insert_8(params)
            t = insert_single(jwt,query)
        exe_time = t
        if j != 0:
            total_time += exe_time
        #param = '|'.join(params)
        line = str(n)+": " +name_data + ", " +  "i_insert_"+str(i)+", " + params[0] + ", " + str(exe_time) + " seconds"
        print(line)
        report += line + "\n"
        if n == 100:
            break;
        n += 1
    report += "summary, " +  "i_insert_"+str(i) + ", " + str(total_time/n) + " seconds\n"
    ofile.write(report)
    ofile.write("\n")
    print (report)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run_ii.py data_name param_path query_index")
        sys.exit()
    run_i_insert(os.path.basename(sys.argv[1]), sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else "")
