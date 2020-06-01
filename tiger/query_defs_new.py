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

import sys, csv, os
from tornado.httputil import url_concat
from datetime import datetime
if sys.version_info < (3, 0):
  from urllib import pathname2url as quote
else:
  from urllib.parse import quote as quote

ENDPOINT_URL_PREFIX = "http://127.0.0.1:9000/query/ldbc_snb/"

def generate_seed_dict(row, query_type, query_num):
  # is queries
  if query_type == "is":
    if query_num in [1,2,3]:
      seed = {"personId":row}
    else:
      seed = {"messageId":row}
  # By rwang: add ii(interactive insert/update) queries
  if query_type == "ii":
    if query_num == 1:
      studyAt_list = row[12].split(";")
      studyAt_id = []
      studyAt_cy = []
      for sa in studyAt_list:
        s = sa.split(",")
        if len(s) == 2:
          studyAt_id.append(s[0])
          studyAt_cy.append(s[1])
      
      workAt_list = row[13].split(";")
      workAt_id = []
      workAt_wf = []
      for wa in workAt_list:
        w = wa.split(",")
        if len(w) == 2:
          workAt_id.append(w[0])
          workAt_wf.append(w[1])
      birthday = datetime.fromtimestamp(int(row[4])/1000).strftime("%Y-%m-%d %H:%M:%S")
      creationDate = datetime.fromtimestamp(int(row[5])/1000).strftime("%Y-%m-%d %H:%M:%S")
      seed = {"personId":row[0], "personFirstName":row[1], "personLastName":row[2], "gender": row[3], "birthday":birthday[0:birthday.find(' ')], "creationDate":creationDate, "locationIP":row[6], "browserUsed":row[7], "cityId":row[8], "languages":row[9].split(";"), "emails": row[10].split(";"), "tagIds":row[11].split(";"), "studyAt": studyAt_id, "workAt": workAt_id}
    elif query_num == 2:
      creationDate = datetime.fromtimestamp(int(row[2])/1000).strftime("%Y-%m-%d %H:%M:%S")
      seed = {"personId":row[0], "postId": row[1], "creationDate": creationDate}
    elif query_num == 3:
      creationDate = datetime.fromtimestamp(int(row[2])/1000).strftime("%Y-%m-%d %H:%M:%S")
      seed = {"personId":row[0], "commentId": row[1], "creationDate": creationDate}
    elif query_num == 4:
      creationDate = datetime.fromtimestamp(int(row[2])/1000).strftime("%Y-%m-%d %H:%M:%S")
      seed = {"forumId":row[0], "forumTitle": row[1], "creationDate": creationDate, "moderatorPersonId":row[3], "tagIds":row[4].split(";")}
    elif query_num == 5:
      creationDate = datetime.fromtimestamp(int(row[2])/1000).strftime("%Y-%m-%d %H:%M:%S")
      seed = {"personId":row[1], "forumId": row[0], "joinDate": creationDate}
    elif query_num == 6:
      tag_ids=[]
      if row[11].strip() != '':
        tag_ids=row[11].strip().split(";")
      creationDate = datetime.fromtimestamp(int(row[2])/1000).strftime("%Y-%m-%d %H:%M:%S")
      seed = {"postId":row[0], "imageFile": row[1], "creationDate": creationDate, "locationIP":row[3], "browserUsed":row[4], "language":row[5], "content":row[6], "length":row[7], "authorPersonId":row[8], "forumId":row[9], "countryId":row[10], "tagIds":tag_ids}
    elif query_num == 7:
      tag_ids=[]
      if row[10].strip() != '':
        tag_ids=row[10].strip().split(";")
      creationDate = datetime.fromtimestamp(int(row[1])/1000).strftime("%Y-%m-%d %H:%M:%S")
      seed = {"commentId":row[0], "creationDate": creationDate, "locationIP":row[2], "browserUsed":row[3], "content":row[4], "length":row[5], "authorPersonId":row[6], "countryId":row[7], "replyToPostId":row[8], "replyToCommentId":row[9], "tagIds":tag_ids}
    elif query_num == 8:
      creationDate = datetime.fromtimestamp(int(row[2])/1000).strftime("%Y-%m-%d %H:%M:%S")
      seed = {"personId":row[0], "person2Id": row[1], "creationDate": creationDate}
  # By rwang: add id(interactive delete) queries
  if query_type == "id":
    if query_num == 1:
      seed = {"personId":row[0]}
    elif query_num == 2:
      seed = {"personId":row[0], "postId": row[1]}
    elif query_num == 3:
      seed = {"personId":row[0], "commentId": row[1]}
    elif query_num == 4:
      seed = {"forumId":row[0]}
    elif query_num == 5:
      seed = {"personId":row[1], "forumId": row[0]}
    elif query_num == 6:
      seed = {"postId":row[0]}
    elif query_num == 7:
      seed = {"commentId":row[0]}
    elif query_num == 8:
      seed = {"person1Id":row[0], "person2Id": row[1]}
  # ic queries
  if query_type == "ic":
    if query_num == 1:
      seed = {"personId":row[0], "firstName":row[1]}
    elif query_num == 2:
      max_date = row[1]
      seed = {"personId":row[0], "maxDate":max_date}
    elif query_num == 3:
      start_date = row[1]
      seed = {"personId":row[0], "startDate":start_date, "durationDays":row[2], "countryXName":row[3], "countryYName":row[4]}
    elif query_num == 4:
      start_date = row[1]
      seed = {"personId":row[0], "startDate":start_date, "durationDays":row[2]}
    elif query_num == 5:
      min_date = row[1]
      seed = {"personId":row[0], "minDate":min_date}
    elif query_num == 6:
      seed = {"personId":row[0], "tagName":row[1]}
    elif query_num == 7:
      seed = {"personId":row[0]}
    elif query_num == 8:
      seed = {"personId":row[0]}
    elif query_num == 9:
      max_date = row[1]
      seed = {"personId":row[0], "maxDate":max_date}
    elif query_num == 10:
      month = int(row[1])
      next_month = (month + 1) if month < 12 else 1
      seed = {"personId":row[0], "month":str(month), "nextMonth":str(next_month)}
    elif query_num == 11:
      seed = {"personId":row[0], "countryName":row[1], "workFromYear":row[2]}
    elif query_num == 12:
      seed = {"personId":row[0], "tagClassName":row[1]}
    elif query_num == 13:
      seed = {"person1Id":row[0], "person2Id":row[1]}
    elif query_num == 14:
      seed = {"person1Id":row[0], "person2Id":row[1]}
  # bi queries
  elif query_type == "bi":
    if query_num == 1:
      max_date = row[0]
      seed = {"maxDate":max_date}
    elif query_num == 2:
      start_date = row[0]
      end_date = row[1]
      seed = {"startDate":start_date, "endDate":end_date, "country1Name":row[2], "country2Name":row[3]}
    elif query_num == 3:
      seed = {"year1":row[0], "month1":row[1]}
    elif query_num == 4:
      seed = {"tagClassName":row[0], "countryName":row[1]}
    elif query_num == 5:
      seed = {"countryName":row[0]}
    elif query_num == 6:
      seed = {"tagName":row[0]}
    elif query_num == 7:
      seed = {"tagName":row[0]}
    elif query_num == 8:
      seed = {"tagName":row[0]}
    elif query_num == 9:
      seed = {"tagClass1Name":row[0], "tagClass2Name":row[1], "threshold":row[2]}
    elif query_num == 10:
      min_date = row[1]
      seed = {"tagName":row[0], "minDate":min_date}
    elif query_num == 11:
      seed = {"countryName":row[0], "blacklist":row[1].split(";")}
    elif query_num == 12:
      date = row[0]
      seed = {"minDate":date, "likeThreshold":row[1]}
    elif query_num == 13:
      seed = {"countryName":row[0]}
    elif query_num == 14:
      start_date = row[0]
      end_date = row[1]
      seed = {"startDate":start_date, "endDate":end_date}
    elif query_num == 15:
      seed = {"countryName":row[0]}
    elif query_num == 16:
      seed = {"personId":row[0], "countryName":row[1], "tagClassName":row[2], "minPathDistance":row[3], "maxPathDistance":row[4]}
    elif query_num == 17:
      seed = {"countryName":row[0]}
    elif query_num == 18:
      min_date = row[0]
      seed = {"minDate":min_date, "lengthThreshold":row[1], "languages":row[2].split(";")}
    elif query_num == 19:
      min_date = row[0]
      seed = {"minDate":min_date, "tagClass1Name":row[1], "tagClass2Name":row[2]}
    elif query_num == 20:
      seed = {"tagClassNames":row[0].split(";")}
    elif query_num == 21:
      end_date = row[1]
      seed = {"countryName":row[0], "endDate":end_date}
    elif query_num == 22:
      seed = {"country1Name":row[0], "country2Name":row[1]}
    elif query_num == 23:
      seed = {"countryName":row[0]}
    elif query_num == 24:
      seed = {"tagClassName":row[0]}
    elif query_num == 25:
      start_date = row[2]
      end_date = row[3]
      seed = {"person1Id":row[0], "person2Id":row[1], "startDate":start_date, "endDate":end_date}
  return seed

def get_endpoint_url(seed, query_type, query_num):
  url_prefix = ENDPOINT_URL_PREFIX + "{}_{}?".format(query_type, query_num)
  args = ""
  for key, value in seed.items():
    if not type(value) is list:
      if type(value) is bytes:
        args += "{}={}&".format(key, quote(value))
      else:
        args += "{}={}&".format(key, value)
    else:
      for v in value:
        args += "{}={}&".format(key, quote(v))
  return url_prefix + args[:-1]
  
def get_endpoints(path, query_type, query_num):
  f_name = "IC_{}.txt".format(query_num) if query_type == "ic" \
      else "BI_{}.txt".format(query_num) if query_type == "bi" \
      else "updateStream_0_0_ii{}.csv".format(query_num) if query_type == "ii" \
      else "updateStream_0_0_ii{}.csv".format(query_num) if query_type == "id" \
      else "interactive_0_0_is{}.csv".format(query_num)
  # By rwang: according our parameter files, modify the format of file reading 
  with open(os.path.join(path, f_name), "r") as f:
    reader = csv.reader(f, delimiter="|")
    if query_type == "ic" or query_type == "bi":
        next(reader) # skip header
    seeds = []
    urls=[]
    for row in reader:
      seed = generate_seed_dict(row, query_type, query_num)
      url = get_endpoint_url(seed, query_type, query_num)
      urls.append(url)
  return urls