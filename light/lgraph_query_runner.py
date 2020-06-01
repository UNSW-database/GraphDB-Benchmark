import sys
import pycurl, json

def i_insert_1(param):
    personId = param[0]
    personFirstName = param[1]
    personLastName = param[2]
    gender = param[3]
    birthday = param[4]
    creationDate = param[5]
    locationIP = param[6]
    browserUsed = param[7]
    cityId = param[8]
    languages = param[9]
    emails = param[10]
    tagIds = param[11]
    studyAt = param[12]
    workAt = param[13]
    query = "MATCH (c:Place {id:"+cityId+"}) \
    CREATE (p:Person {id: "+personId+", firstName: "+personFirstName+", lastName: "+personLastName+", gender: "+gender+", birthday: "+birthday+", creationDate: "+creationDate+", locationIP: "+locationIP+", browserUsed: "+browserUsed+", languages: "+languages+", emails: "+emails+"})-[:IS_LOCATED_IN]->(c) WITH p, count(*) AS dummy1 UNWIND "+tagIds+" AS tagId MATCH (t:Tag {id: tagId}) CREATE (p)-[:HAS_INTEREST]->(t) WITH p, count(*) AS dummy2 UNWIND "+studyAt+" AS s MATCH (u:Organisation {id: s}) CREATE (p)-[:STUDY_AT {classYear: 0}]->(u) WITH p, count(*) AS dummy3 UNWIND "+workAt+" AS w MATCH (comp:Organisation {id: w}) CREATE (p)-[:WORK_AT {workFrom: 0}]->(comp);"
    return query
    

def i_delete_1(param):
    personId = param[0]
    query = "MATCH (p:Person { id: "+personId+" }) DETACH DELETE p;"
    return query
    
def i_check_1(param):
    personId = param[0]
    query = "MATCH (p:Person { id: "+personId+" }) RETURN p;"
    return query
    
def i_insert_2(param):
    personId = param[0]
    postId = param[1]
    creationDate = param[2]
    query = "MATCH (person:Person {id:"+personId+"}),(post:Post {id:"+postId+"}) \
    CREATE (person)-[:LIKES {creationDate:"+creationDate+"}]->(post);"
    return query
    
def i_delete_2(param):
    personId = param[0]
    postId = param[1]
    query = "MATCH (person:Person {id:"+personId+"})-[like:LIKES]->(post:Post {id:"+postId+"}) DELETE like;"
    return query
    
def i_check_2(param):
    personId = param[0]
    postId = param[1]
    query = "MATCH (person:Person {id:"+personId+"})-[like:LIKES]->(post:Post {id:"+postId+"}) RETURN like;"
    return query
    
def i_insert_3(param):
    personId = param[0]
    commentId = param[1]
    creationDate = param[2]
    query = "MATCH (person:Person {id:"+personId+"}),(comment:Comment {id:"+commentId+"}) \
    CREATE (person)-[:LIKES {creationDate:"+creationDate+"}]->(comment);"
    return query
    
def i_delete_3(param):
    personId = param[0]
    commentId = param[1]
    query = "MATCH (person:Person {id:"+personId+"})-[like:LIKES]->(comment:Comment {id:"+commentId+"}) DELETE like;"
    return query

def i_check_3(param):
    personId = param[0]
    commentId = param[1]
    query = "MATCH (person:Person {id:"+personId+"})-[like:LIKES]->(comment:Comment {id:"+commentId+"}) RETURN like;"
    return query
    
def i_insert_4(param):
    forumId = param[0]
    forumTitle = param[1]
    creationDate = param[2]
    moderatorPersonId = param[3]
    tagIds = param[4]
    query = "MATCH (p:Person {id: "+moderatorPersonId+"}) \
    CREATE (f:Forum {id: "+forumId+", title: "+forumTitle+", creationDate: "+creationDate+"})-[:HAS_MODERATOR]->(p) \
    WITH f UNWIND "+tagIds+" AS tagId MATCH (t:Tag {id: tagId}) CREATE (f)-[:HAS_TAG]->(t);"
    return query
    
def i_delete_4(param):
    forumId = param[0]
    query = "MATCH (f:Forum { id: "+forumId+" }) DETACH DELETE f;"
    return query
    
def i_check_4(param):
    forumId = param[0]
    query = "MATCH (f:Forum { id: "+forumId+" }) RETURN f;"
    return query

def i_insert_5(param):
    personId = param[1]
    forumId = param[0]
    joinDate = param[2]
    query = "MATCH (f:Forum {id:"+forumId+"}), (p:Person {id:"+personId+"}) CREATE (f)-[:HAS_MEMBER {joinDate:"+joinDate+"}]->(p);"
    return query
    
def i_delete_5(param):
    personId = param[1]
    forumId = param[0]
    query = "MATCH (f:Forum {id:"+forumId+"})-[h:HAS_MEMBER]->(p:Person {id:"+personId+"}) DELETE h;"
    return query
    
def i_check_5(param):
    personId = param[1]
    forumId = param[0]
    query = "MATCH (f:Forum {id:"+forumId+"})-[h:HAS_MEMBER]->(p:Person {id:"+personId+"}) RETURN h;"
    return query
    
def i_insert_6(param):
    postId = param[0]
    imageFile = param[1]
    creationDate = param[2]
    locationIP = param[3]
    browserUsed = param[4]
    language = param[5]
    content = param[6]
    length = param[7]
    authorPersonId = param[8]
    forumId = param[9]
    countryId = param[10]
    tagIds = param[11]
    query = "MATCH (author:Person {id: "+authorPersonId+"}), (country:Place {id: "+countryId+"}), (forum:Forum {id: "+forumId+"}) \
    CREATE (author)<-[:HAS_CREATOR]-(p:Post {id: "+postId+", creationDate: "+creationDate+", locationIP: "+locationIP+", browserUsed: "+browserUsed+", languages: "+language+", content: "+content+", imageFile: "+imageFile+", length: "+length+"})<-[:CONTAINER_OF]-(forum), (p)-[:IS_LOCATED_IN]->(country) \
    WITH p UNWIND "+tagIds+" AS tagId MATCH (t:Tag {id: tagId}) CREATE (p)-[:HAS_TAG]->(t);"
    print(query)
    return query
    
def i_delete_6(param):
    postId = param[0]
    query = "MATCH (p:Post { id: "+postId+" }) DETACH DELETE p;"
    return query
    
def i_check_6(param):
    postId = param[0]
    query = "MATCH (p:Post { id: "+postId+" }) RETURN p;"
    return query

def i_insert_7(param):
    commentId = param[0]
    creationDate = param[1]
    locationIP = param[2]
    browserUsed = param[3]
    content = param[4]
    length = param[5]
    authorPersonId = param[6]
    countryId = param[7]
    replyToPostId = param[8]
    replyToCommentId = param[9]
    tagIds = param[10]
    query = "MATCH (author:Person {id: "+authorPersonId+"}), (country:Place {id: "+countryId+"}),"
    if replyToPostId != '-1':
        query += "(message:Post {id: "+replyToPostId+"}) "
    else:
        query += "(message:Comment {id: "+replyToCommentId+"}) "
    query += "CREATE (author)<-[:HAS_CREATOR]-(c:Comment {id: "+commentId+", creationDate: "+creationDate+", locationIP: "+locationIP+", browserUsed: "+browserUsed+", content: "+content+", length: "+length+"})-[:REPLY_OF]->(message), (c)-[:IS_LOCATED_IN]->(country) \
    WITH c UNWIND "+tagIds+" AS tagId MATCH (t:Tag {id: tagId}) CREATE (c)-[:HAS_TAG]->(t);"
    return query
    
def i_delete_7(param):
    commentId = param[0]
    query = "MATCH (c:Comment { id: "+commentId+" }) DETACH DELETE c;"
    return query

def i_check_7(param):
    commentId = param[0]
    query = "MATCH (c:Comment { id: "+commentId+" }) RETURN c;"
    return query
    
def i_insert_8(param):
    person1Id = param[0]
    person2Id = param[1]
    creationDate = param[2]
    query = "MATCH (p1:Person {id:"+person1Id+"}), (p2:Person {id:"+person2Id+"}) CREATE (p1)-[:KNOWS {creationDate:"+creationDate+"}]->(p2);"
    return query
    
def i_delete_8(param):
    person1Id = param[0]
    person2Id = param[1]
    query = "MATCH (p1:Person {id:"+person1Id+"})-[k:KNOWS]->(p2:Person {id:"+person2Id+"}) DELETE k;"
    return query
    
def i_check_8(param):
    person1Id = param[0]
    person2Id = param[1]
    query = "MATCH (p1:Person {id:"+person1Id+"})-[k:KNOWS]->(p2:Person {id:"+person2Id+"}) RETURN k;"
    return query
        
def is_1(param):
    personId = param
    query = "MATCH (n:Person {id:" + personId + "})-[:IS_LOCATED_IN]->(p:Place) \
    RETURN n.firstName AS firstName, n.lastName AS lastName, n.birthday AS birthday, n.locationIP AS locationIP, \
    n.browserUsed AS browserUsed, p.id AS cityId, n.gender AS gender, n.creationDate AS creationDate;"
    return query

def is_2(param):
    personId = param
    query = "MATCH (:Person {id:"+personId+"})<-[:HAS_CREATOR]-(m)-[:REPLY_OF*0..]->(p:Post), \
    (p)-[:HAS_CREATOR]->(c) \
    RETURN m.id as messageId, CASE exists(m.content) WHEN true THEN m.content ELSE m.imageFile END AS messageContent, \
    m.creationDate AS messageCreationDate, p.id AS originalPostId, c.id AS originalPostAuthorId, \
    c.firstName as originalPostAuthorFirstName, c.lastName as originalPostAuthorLastName \
    ORDER BY messageCreationDate DESC LIMIT 10;"
    return query

def is_3(param):
    personId = param
    query = "MATCH (n:Person {id:"+personId+"})-[r:KNOWS]-(friend) \
    RETURN friend.id AS personId, friend.firstName AS firstName, friend.lastName AS lastName, \
    r.creationDate AS friendshipCreationDate ORDER BY friendshipCreationDate DESC, personId ASC;"
    return query

def is_4(param):
    messageId = param
    query = "MATCH (m {id:"+messageId+"}) WHERE m:Comment OR m:Post \
    RETURN m.creationDate as messageCreationDate, CASE exists(m.content) WHEN true THEN m.content ELSE m.imageFile END AS messageContent;"
    return query

def is_5(param):
    messageId = param
    query = "MATCH (m {id:"+messageId+"})-[:HAS_CREATOR]->(p:Person) \
    RETURN p.id AS personId, p.firstName AS firstName, p.lastName AS lastName;"
    return query

def is_6(param):
    messageId = param
    query = "MATCH (m {id:"+messageId+"})-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person) \
    RETURN f.id AS forumId, f.title AS forumTitle, mod.id AS moderatorId, \
    mod.firstName AS moderatorFirstName, mod.lastName AS moderatorLastName;"
    return query

def is_7(param):
    messageId = param
    query = "MATCH (m {id:"+messageId+"})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person) WITH m, c, p \
    OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p) \
    RETURN c.id AS commentId, c.content AS commentContent, c.creationDate AS commentCreationDate, \
    p.id AS replyAuthorId, p.firstName AS replyAuthorFirstName, p.lastName AS replyAuthorLastName, \
    CASE r WHEN null THEN false ELSE true END AS replyAuthorKnowsOriginalMessageAuthor \
    ORDER BY commentCreationDate DESC, replyAuthorId;"
    return query

if __name__ == "__main__":
    c = pycurl.Curl()
    c.setopt(pycurl.URL, query_url)
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.HTTPHEADER, head)
    param = ['17592186053137']
    query = is_2(param)
    data = json.dumps({"script":query})
    c.setopt(pycurl.POSTFIELDS, data)
    result = c.perform_rb()
    result = json.loads(result)
    time = result["elapsed"]
    header = result["header"]
    output = result["result"]
    print(time)
    print(header)
    print(output)
    c.close()
