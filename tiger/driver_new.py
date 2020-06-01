############################################################
# Copyright (c)  2015-now, TigerGraph Inc.
# All rights reserved
# It is provided as it is for benchmark reproducible purpose.
# anyone can use it for benchmark purpose with the 
# acknowledgement to TigerGraph.
############################################################

############################################################
# The orginal script is provided by TigerGraph.
# We add and modify some code 
# to adapt this benchmark to our experiment.
# The modified part is noted with 'By rwang:......'
############################################################

import sys, logging, time
from datetime import timedelta
from json import loads
from argparse import ArgumentParser

from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPClientError

from query_defs_new import *

# default value for arguments
DEFAULT_MAX_NUM_SEEDS = 3

# params for interactive update queries
II_NAME = "Interactive Insert"
II_SIZE = 8

ID_NAME = "Interactive Delete"
ID_SIZE = 8

# params for interactive short queries
IS_NAME = "Interactive Short"
IS_SIZE = 7

# params for ic queries
IC_NAME = "Interactive Complex"
IC_SIZE = 14

# params for bi queries
BI_NAME = "Business Intelligence"
BI_SIZE = 25

INFO_LVL_1 = 11
INFO_LVL_2 = 12

def info1(self, message, *args, **kws):
  if self.isEnabledFor(INFO_LVL_1):
    self._log(INFO_LVL_1, message, args, **kws) 

def info2(self, message, *args, **kws):
  if self.isEnabledFor(INFO_LVL_2):
    self._log(INFO_LVL_2, message, args, **kws) 

def handle_response(response):
  response_time = 0;
  has_error = False
  logging.Logger.info1(logging.root, "\n[Request] {}".format(response.request.url))

  if response.error:
    has_error = True # this should be catched by the caller first
  else:
    response_json = loads(response.body.decode("utf-8"))
    if response_json["error"]:
      has_error = True
      if "code" in response_json:
        print("-- Error {}: {}".format(response_json["code"] , response_json["message"]))
      else:
        print("-- {}".format(response_json["message"]))
    else:
      response_time = response.time_info["starttransfer"] - response.time_info["pretransfer"]
      logging.Logger.info1(logging.root, "[Response] {}".format(response_json["results"]))
      logging.Logger.info2(logging.root, "[Running Time] {} sec".format(round(response_time, 10)))

  return response_time, has_error

def run_query(http_client, path, num, seed, query_type, query_num, person_ids=[], message_ids=[]):
  response_recv = 0
  response_time = 0
  has_error = None

  print("- {} {}:".format(
      IS_NAME if query_type == "is" else II_NAME if query_type == "ii" else ID_NAME if query_type == "id" else IC_NAME if query_type == "ic" else BI_NAME, query_num))

  urls = []
  urls = get_endpoints(path, query_type, query_num)
  # By rwang: is、ii、id queries process different 100 urls(diff parameters)
  # By rwang: ic & id queries process the same url(same parameters) $num times
  if (query_type == "ic" or query_type == "bi") and len(urls) == 1:
    url = urls[0]
    urls = []
    for i in range(num):
      urls.append(url)

  request_sent = len(urls)

  try:
    for i in range(len(urls)):
      http_client = HTTPClient()
      response = http_client.fetch(urls[i], method="GET", connect_timeout=3600, request_timeout=3600)
      t, has_error = handle_response(response)
      time.sleep(3)
      http_client.close()
      if not has_error: # delete the first time result
        print("-- Each Response Time: {}, {} sec\n".format(i, t))
        #By rwang: response time exclude the first processing time
        if i != 0: 
          response_time += t
          response_recv += 1
      else:
        break

    if not has_error:
      print("\n-- # {}: {}".format("Seeds" if not seed else "Iterations", request_sent))
      if response_recv > 0:
        print("-- Average Response Time: {} sec\n".format(round((response_time/response_recv),10)))
  except HTTPClientError as e:
    print("-- Bad Response: HTTP {} {}".format(e.response.code, e.response.reason))
  except Exception as e:
    print("-- Unexpected Error:\n{}".format(repr(e)))

def run_all_is(http_client, path, num):
  for i in range(1,IS_SIZE+1):
    run_query(http_client, path, num, None, "is", i)

# By rwang: add function run_all_ii
def run_all_ii(http_client, path, num):
  for i in range(1,II_SIZE+1):
    run_query(http_client, path, num, None, "ii", i)

# By rwang: add function run_all_id
def run_all_id(http_client, path, num):
  for i in range(1,ID_SIZE+1):
    run_query(http_client, path, num, None, "id", i)
    
def run_all_ic(http_client, path, num):
  for i in range(1,IC_SIZE+1):
    run_query(http_client, path, num, None, "ic", i)

def run_all_bi(http_client, path, num):
  for i in range(1,BI_SIZE+1):
    run_query(http_client, path, num, None, "bi", i)


if __name__ == "__main__":
  # this is required to retrieve response.time_info even if we use the blocking HTTPClient
  AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

  ap = ArgumentParser()
  ap.add_argument("-p", "--path", required=True, 
      help="Full path to the seed directory.")
  #By rwang: redefine the parameter 'num'
  ap.add_argument("-n", "--num", type=int, default=DEFAULT_MAX_NUM_SEEDS, 
      help="Number of times to run IC & BI queries.") 
  ap.add_argument("-q", "--query", required=True,
      help="Type and/or number of query(ies) to run, \
          e.g. IS_2, Ic_12, bi_22 to run a specific query, \
          and is, iC, BI to run all queries in the given workload")
  ap.add_argument("-d", "--debug", type=int, help="Show HTTP request/response.")

  args = ap.parse_args()

  logging.addLevelName(INFO_LVL_1, "INFO1")
  logging.Logger.info1 = info1
  logging.addLevelName(INFO_LVL_2, "INFO2")
  logging.Logger.info2 = info2

  if args.debug:
    logging.basicConfig(stream=sys.stdout, level=(INFO_LVL_1 if args.debug == 1 else INFO_LVL_2), 
        format="%(message)s")

  # here we use blocking HTTP client since response time in AsyncHTTPClient is unstable
  # as the number of clients increases.
  http_client = HTTPClient()

  if args.query: 
    query_info = args.query.split("_")
    query_type = query_info[0].lower()
    if len(query_info) == 2:
      query_num = int(query_info[1])
    else:
      query_num = 0

    if query_type == "is":
      if query_num in range(1,IS_SIZE+1):
        run_query(http_client, args.path, args.num, args.seed, "is", query_num, [], [])
      else:
        run_all_is(http_client, args.path, args.num)
    # By rwang: add ii(interactive insert/update) query
    elif query_type == "ii": 
      if query_num in range(1,II_SIZE+1):
        run_query(http_client, args.path, args.num, args.seed, "ii", query_num, [], [])
      else:
        run_all_ii(http_client, args.path, args.num)
    # By rwang: add id(interactive delete) query
    elif query_type == "id": 
      if query_num in range(1,ID_SIZE+1):
        run_query(http_client, args.path, args.num, args.seed, "id", query_num, [], [])
      else:
        run_all_id(http_client, args.path, args.num)
    elif query_type == "ic":
      if query_num in range(1,IC_SIZE+1):
        run_query(http_client, args.path, args.num, args.seed, "ic", query_num, [], [])
      else:
        run_all_ic(http_client, args.path, args.num)
    elif query_type == "bi":
      if query_num in range(1,BI_SIZE+1):
        run_query(http_client, args.path, args.num, args.seed, "bi", query_num, [], [])
      else:
        run_all_bi(http_client, args.path, args.num)
    else:
      print(args.query + " does not exist")
    
  http_client.close()
