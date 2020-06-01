import psycopg2
import agensgraph

class AgensQueryRunner(object):
    def __init__(self):
        self.conn = psycopg2.connect("dbname=ran host=127.0.0.1 user=ran")
        self.cur = self.conn.cursor()
        self.cur.execute("SET graph_path=ldbc_graph;")
        self.cur.execute("SET statement_timeout = '3600s';") # 

    def i_insert_1(self, param):
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
        query_0 = "CREATE (:Person {id: "+personId+", firstName: "+personFirstName+", lastName: "+personLastName+", gender: "+gender+", birthday: "+birthday+", creationDate: "+creationDate+", locationIP: "+locationIP+", browserUsed: "+browserUsed+", languages: "+languages+", emails: "+emails+"});"
        self.cur.execute(query_0)
        query_1 = "MATCH (p:Person{id: "+personId+"}), (c:Place{id:"+cityId+"}) \
        CREATE (p)-[:IS_LOCATED_IN]->(c);"
        self.cur.execute(query_1)
        query_2 = "MATCH (p:Person{id: "+personId+"}) WITH p UNWIND "+tagIds+" AS tagId\
        MATCH (t:Tag{id:tagId}) CREATE (p)-[:HAS_INTEREST]->(t);"
        self.cur.execute(query_2)
        query_3 = "MATCH (p:Person{id: "+personId+"}) WITH p UNWIND "+studyAt+" AS s \
        MATCH (u:Organisation {id: s[0]}) WITH p,u,s[1] AS cy CREATE (p)-[:STUDY_AT {classYear: cy}]->(u);"
        self.cur.execute(query_3)
        query_4 = "MATCH (p:Person{id: "+personId+"}) WITH p UNWIND "+workAt+" AS w \
        MATCH (comp:Organisation {id: w[0]}) WITH p,comp,w[1] AS wf CREATE (p)-[:WORKS_AT {workFrom: wf}]->(comp);"
        self.cur.execute(query_4)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_delete_1(self, param):
        personId = param[0]
        query = "MATCH (p:Person { id: "+personId+" }) DETACH DELETE p;"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_check_1(self, param):
        personId = param[0]
        query = "MATCH (p:Person { id: "+personId+" }) RETURN p;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result
        
    def i_insert_2(self, param):
        personId = param[0]
        postId = param[1]
        creationDate = param[2]
        query = "MATCH (person:Person {id:"+personId+"}),(post:Post {id:"+postId+"}) \
        CREATE (person)-[:LIKES {creationDate:"+creationDate+"}]->(post);"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_delete_2(self, param):
        personId = param[0]
        postId = param[1]
        query = "MATCH (person:Person {id:"+personId+"})-[l:LIKES]->(post:Post {id:"+postId+"}) DELETE l;"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_check_2(self, param):
        personId = param[0]
        postId = param[1]
        query = "MATCH (person:Person {id:"+personId+"})-[l:LIKES]->(post:Post {id:"+postId+"}) RETURN l;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result
        
    def i_insert_3(self, param):
        personId = param[0]
        commentId = param[1]
        creationDate = param[2]
        query = "MATCH (person:Person {id:"+personId+"}),(comment:Comment {id:"+commentId+"}) \
        CREATE (person)-[:LIKES {creationDate:"+creationDate+"}]->(comment);"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_delete_3(self, param):
        personId = param[0]
        commentId = param[1]
        query = "MATCH (person:Person {id:"+personId+"})-[l:LIKES]->(comment:Comment {id:"+commentId+"}) DELETE l;"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
   
    def i_check_3(self, param):
        personId = param[0]
        commentId = param[1]
        query = "MATCH (person:Person {id:"+personId+"})-[l:LIKES]->(comment:Comment {id:"+commentId+"}) RETURN l;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result
        
    def i_insert_4(self, param):
        forumId = param[0]
        forumTitle = param[1]
        creationDate = param[2]
        moderatorPersonId = param[3]
        tagIds = param[4]
        query_0 = "CREATE (:Forum {id: "+forumId+", title: "+forumTitle+", creationDate: "+creationDate+"});"
        self.cur.execute(query_0)
        query_1 = "MATCH (p:Person {id: "+moderatorPersonId+"}), (f:Forum {id: "+forumId+"}) \
        CREATE (f)-[:HAS_MODERATOR]->(p);"
        self.cur.execute(query_1)
        query_2 = "MATCH (f:Forum{id: "+forumId+"}) WITH f UNWIND "+tagIds+" AS tagId MATCH (t:Tag {id: tagId}) CREATE (f)-[:HAS_TAG]->(t);"
        self.cur.execute(query_2)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_delete_4(self, param):
        forumId = param[0]
        query = "MATCH (f:Forum { id: "+forumId+" }) DETACH DELETE f;"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_check_4(self, param):
        forumId = param[0]
        query = "MATCH (f:Forum { id: "+forumId+" }) RETURN f;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result
        
    def i_insert_5(self, param):
        personId = param[1]
        forumId = param[0]
        joinDate = param[2]
        query = "MATCH (f:Forum {id:"+forumId+"}), (p:Person {id:"+personId+"}) CREATE (f)-[:HAS_MEMBER {joinDate:"+joinDate+"}]->(p);"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_delete_5(self, param):
        personId = param[1]
        forumId = param[0]
        query = "MATCH (f:Forum {id:"+forumId+"})-[h:HAS_MEMBER]->(p:Person {id:"+personId+"}) \
        DELETE h;"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_check_5(self, param):
        personId = param[1]
        forumId = param[0]
        query = "MATCH (f:Forum {id:"+forumId+"})-[h:HAS_MEMBER]->(p:Person {id:"+personId+"}) \
        RETURN h;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result
        
    def i_insert_6(self, param):
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
        query_0="CREATE (:Post {id: "+postId+", creationDate: "+creationDate+", locationIP: "+locationIP+", browserUsed: "+browserUsed+", languages: "+language+", content: "+content+", imageFile: "+imageFile+", length: "+length+"});"
        self.cur.execute(query_0)
        query_1 = "MATCH (author:Person {id: "+authorPersonId+"}), (country:Place {id: "+countryId+"}), (forum:Forum {id: "+forumId+"}),(p:Post{id:"+postId+"}) \
        CREATE (author)<-[:HAS_CREATOR]-(p)<-[:CONTAINER_OF]-(forum), (p)-[:IS_LOCATED_IN]->(country);"
        self.cur.execute(query_1)
        query_2 = "MATCH (p:Post{id:"+postId+"}) WITH p UNWIND "+tagIds+" AS tagId MATCH (t:Tag {id: tagId}) CREATE (p)-[:HAS_TAG]->(t)"
        self.cur.execute(query_2)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_delete_6(self, param):
        postId = param[0]
        query = "MATCH (p:Post { id: "+postId+" }) DETACH DELETE p;"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_check_6(self, param):
        postId = param[0]
        query = "MATCH (p:Post { id: "+postId+" }) RETURN p;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result
        
    def i_insert_7(self, param):
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
        query_0="CREATE (:Comment {id: "+commentId+", creationDate: "+creationDate+", locationIP: "+locationIP+", browserUsed: "+browserUsed+", content: "+content+", length: "+length+"});"
        self.cur.execute(query_0)
        #print query_0
        query_1 = "MATCH (author:Person {id: "+authorPersonId+"}), (country:Place {id: "+countryId+"}), \
        (message:Message {id: "+replyToPostId+" + "+replyToCommentId+" + 1}), (c:Comment{id:"+commentId+"}) \
        CREATE (author)<-[:HAS_CREATOR]-(c)-[:REPLY_OF]->(message), (c)-[:IS_LOCATED_IN]->(country);"
        self.cur.execute(query_1)
        query_2 = "MATCH (c:Comment{id:"+commentId+"}) WITH c UNWIND "+tagIds+" AS tagId MATCH (t:Tag {id: tagId}) CREATE (c)-[:HAS_TAG]->(t);"
        self.cur.execute(query_2)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_delete_7(self, param):
        commentId = param[0]
        query = "MATCH (c:Comment { id: "+commentId+" }) DETACH DELETE c;"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result

    def i_check_7(self, param):
        commentId = param[0]
        query = "MATCH (c:Comment { id: "+commentId+" }) RETURN c;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result
        
    def i_insert_8(self, param):
        person1Id = param[0]
        person2Id = param[1]
        creationDate = param[2]
        query = "MATCH (p1:Person {id:"+person1Id+"}), (p2:Person {id:"+person2Id+"}) CREATE (p1)-[:KNOWS {creationDate:"+creationDate+"}]->(p2);"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_delete_8(self, param):
        person1Id = param[0]
        person2Id = param[1]
        query = "MATCH (p1:Person {id:"+person1Id+"})-[k:KNOWS]->(p2:Person {id:"+person2Id+"}) \
        DELETE k;"
        self.cur.execute(query)
        self.conn.commit()
        #result = self.cur.fetchall()
        #print result
        #return result
        
    def i_check_8(self, param):
        person1Id = param[0]
        person2Id = param[1]
        query = "MATCH (p1:Person {id:"+person1Id+"})-[k:KNOWS]->(p2:Person {id:"+person2Id+"}) \
        RETURN k;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result
        
    def i_short_1(self, param):
        personId = param
        query = "MATCH (n:Person {id:" + personId + "})-[:IS_LOCATED_IN]->(p:Place) \
        RETURN n.firstName AS firstName, \
        n.lastName AS lastName, \
        n.birthday AS birthday, \
        n.locationIP AS locationIP, \
        n.browserUsed AS browserUsed, \
        p.id AS cityId, \
        n.gender AS gender, \
        n.creationDate AS creationDate;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_short_2(self, param):
        personId = param
        query = "MATCH (person:Person{id:"+personId+"})<-[:HAS_CREATOR]-(m:Message) \
        WITH m ORDER BY m.creationDate DESC LIMIT 10 \
        MATCH (m)-[:REPLY_OF*0..]->(p:Post)-[:HAS_CREATOR]->(c:Person) \
        RETURN m.id as messageId, COALESCE(m.content, m.imageFile), m.creationDate AS messageCreationDate, \
        p.id AS originalPostId, c.id AS originalPostAuthorId, c.firstName as originalPostAuthorFirstName, \
        c.lastName as originalPostAuthorLastName ORDER BY messageCreationDate DESC LIMIT 10;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_short_3(self, param):
        personId = param
        query = "MATCH (n:Person {id:" + personId + "})-[r:KNOWS]-(friend) \
        RETURN friend.id AS personId, \
        friend.firstName AS firstName, \
        friend.lastName AS lastName, \
        r.creationDate AS friendshipCreationDate \
        ORDER BY friendshipCreationDate DESC, personId ASC;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_short_4(self, param):
        messageId = param
        query = "MATCH (m: Message{id:"+messageId+"}) \
        RETURN COALESCE(m.content, m.imageFile), m.creationDate as creationDate;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_short_5(self, param):
        messageId = param
        query = "MATCH (m:Message {id:" + messageId + "})-[:HAS_CREATOR]->(p:Person) \
        RETURN p.id AS personId, p.firstName AS firstName, p.lastName AS lastName;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_short_6(self, param):
        messageId = param
        query = "MATCH (m:Message {id:" + messageId + "})-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person) \
        RETURN f.id AS forumId, f.title AS forumTitle, \
        mod.id AS moderatorId, mod.firstName AS moderatorFirstName, mod.lastName AS moderatorLastName;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_short_7(self, param):
        messageId = param
        query = "MATCH (m:Message{id:" + messageId + "})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person) \
        OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p) \
        RETURN c.id AS commentId, c.content AS commentContent, c.creationDate AS commentCreationDate, \
        p.id AS replyAuthorId, p.firstName AS replyAuthorFirstName, p.lastName AS replyAuthorLastName, \
        CASE r IS NULL WHEN true THEN false ELSE true END AS replyAuthorKnowsOriginalMessageAuthor \
        ORDER BY commentCreationDate DESC, replyAuthorId ASC;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_1(self, param):
        personId = param[0]
        firstName = param[1]
        query = "MATCH (p:Person{id:"+personId+" })-[path:KNOWS*1..3]-(friend:Person {firstName: "+firstName+"}) \
        WITH friend, min(length(path)) AS distance \
        ORDER BY distance ASC, friend.lastName ASC, friend.id ASC LIMIT 20 \
        MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:Place) \
        OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:Organisation)-[:IS_LOCATED_IN]->(uniCity:Place) \
        WITH friend, collect(CASE uni.name is null WHEN true THEN null ELSE [uni.name, studyAt.classYear, uniCity.name] END) AS unis, \
        friendCity, distance \
        OPTIONAL MATCH (friend)-[worksAt:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(companyCountry:Place) \
        WITH friend, collect(CASE company.name is null WHEN true THEN null ELSE [company.name, worksAt.workFrom, companyCountry.name] END) AS companies, \
        unis, friendCity, distance \
        RETURN friend.id AS id, friend.lastName AS lastName, distance, friend.birthday AS birthday, \
        friend.creationDate AS creationDate, friend.gender AS gender, friend.browserUsed AS browser, \
        friend.locationIp AS locationIp, friend.email AS emails, friend.speaks AS languages, \
        friendCity.name AS cityName, unis, companies ORDER BY distance ASC, lastName ASC, id ASC;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_2(self, param):
        personId = param[0]
        maxDate = param[1]
        query = "MATCH (p:Person{id:"+personId+"})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message) \
        WHERE message.creationDate <= "+maxDate+" \
        WITH friend, message ORDER BY message.creationDate DESC, message.id ASC LIMIT 20 \
        RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, \
        message.id AS messageId, COALESCE(message.content, message.imageFile), \
        message.creationDate AS messageCreationDate;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_3(self, param):
        personId = param[0]
        startDate = param[1]
        endDate = param[2]
        countryX = param[3]
        countryY = param[4]
        query = "MATCH (person:Person{id:"+personId+"})-[:KNOWS*1..2]-(friend:Person) \
        WHERE id(person) != id(friend) \
        WITH DISTINCT friend \
        MATCH (friend)<-[:HAS_CREATOR]-(messageX:Message)-[:IS_LOCATED_IN]->(countryX:Place {name: "+countryX+"}) \
        WHERE not exists ((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryX)) \
        AND messageX.creationDate >= "+startDate+" AND messageX.creationDate < "+endDate+" \
        WITH friend, count(DISTINCT messageX) AS xCount \
        MATCH (friend)<-[:HAS_CREATOR]-(messageY:Message)-[:IS_LOCATED_IN]->(countryY:Place {name: "+countryY+"}) \
        WHERE not exists ((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryY)) \
        AND messageY.creationDate >= "+startDate+" AND messageY.creationDate < "+endDate+" \
        WITH friend, xCount, count(DISTINCT messageY) AS yCount \
        RETURN friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName, \
        xCount, yCount, xCount + yCount AS xyCount \
        ORDER BY xyCount DESC, friendId ASC LIMIT 20;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_4(self, param):
        personId = param[0]
        startDate = param[1]
        endDate = param[2]
        query = "SELECT tagname, count(distinct postid) AS postcount FROM ( \
        SELECT T1.tag AS tagname, T1.postid AS postid, count(distinct T2.postid) AS oldPostCount FROM ( \
        MATCH (person:Person{id:"+personId+"})-[:KNOWS]-()<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag) \
        WHERE post.creationDate >= "+startDate+" AND post.creationDate < "+endDate+" \
        RETURN post.id AS postid, tag.name AS tag ) T1 LEFT JOIN ( \
        MATCH (person:Person{id:"+personId+"})-[:KNOWS]-()<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag) \
        WHERE post.creationDate < "+startDate+" \
        RETURN post.id AS postid, tag.name AS tag ) \
        T2 ON T1.tag = T2.tag GROUP BY T1.tag, T1.postid ) A \
        WHERE oldPostCount = 0 GROUP BY tagname \
        ORDER BY postcount DESC, tagname ASC LIMIT 10;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_5(self, param):
        personId = param[0]
        minDate = param[1]
        query = "MATCH (person:Person{id:"+personId+"})-[:KNOWS*1..2]-(friend:Person) \
        WHERE id(person) != id(friend) WITH DISTINCT friend \
        MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum) \
        WHERE membership.joinDate > "+minDate+" \
        OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum) \
        WITH forum.id AS forumid, forum.title AS forumTitle, count(id(post)) AS postcount \
        ORDER BY postCount DESC, forumid ASC RETURN forumTitle, postCount LIMIT 20;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_6(self, param):
        personId = param[0]
        tagClass = param[1]
        query = "MATCH (person:Person{id:"+personId+"})-[:KNOWS*1..2]-(friend:Person) \
        WHERE id(person) != id(friend) WITH DISTINCT friend \
        MATCH (friend)<-[:HAS_CREATOR]-(friendPost:Post) \
        MATCH (friendPost)-[:HAS_TAG]->(:Tag {name: "+tagClass+"}) \
        MATCH (friendPost)-[:HAS_TAG]->(commonTag:Tag) \
        WHERE commonTag.name <> "+tagClass+" \
        RETURN commonTag.name AS tagName, count(distinct id(friendPost)) AS postCount \
        ORDER BY postCount DESC, tagName ASC LIMIT 10;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_7(self, param): # some result error
        personId = param[0]
        query = "MATCH (person:Person)<-[:HAS_CREATOR]-(message:Message)<-[l:LIKES]-(liker:Person) \
        WHERE person.id = "+personId+" \
        WITH DISTINCT id(liker) AS liker_id, liker, message, l.creationDate AS likeTime, person \
        ORDER BY liker_id, likeTime DESC \
        RETURN liker.Id AS personId, liker.FirstName, liker.LastName, likeTime, \
        message.id AS messageId, COALESCE(message.content, message.imagefile) AS messageContent, \
        not exists((person)-[:KNOWS]-(liker)) ORDER BY likeTime DESC, personId ASC LIMIT 20;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_8(self, param):
        personId = param[0]
        query = "MATCH (p:Person{id:"+personId+"})<-[:HAS_CREATOR]-()<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(person:Person) \
        WITH person, c ORDER BY c.creationDate DESC, c.id ASC LIMIT 20 \
        RETURN person.id AS personId, person.firstName AS personFirstName, person.lastName AS personLastName, \
        c.creationDate AS commentCreationDate, c.id AS commentId, c.content AS commentContent;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_9(self, param):
        personId = param[0]
        maxDate = param[1]
        query = "MATCH (person:Person{id:"+personId+"})-[:KNOWS*1..2]-(friend:Person) \
        WITH DISTINCT friend \
        MATCH (friend)<-[:HAS_CREATOR]-(message:Message) WHERE message.creationDate < "+maxDate+" \
        WITH friend, message \
        ORDER BY message.creationDate DESC, message.id ASC LIMIT 20 \
        RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, \
        message.id AS messageId, COALESCE(message.content, message.imageFile), message.creationDate AS messageCreationDate;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_10(self, param):
        personId = param[0]
        month = param[1]
        next_month = str(int(month) + 1)
        query = "MATCH (person:Person{id:" +personId+ "})-[:KNOWS*2..2]-(friend:Person) \
        WHERE ((friend.birthday/100%100 = "+month+" AND friend.birthday%100 >= 21) \
        OR (friend.birthday/100%100 = "+next_month+" AND friend.birthday%100 < 22)) \
        AND id(friend) != id(person) AND not exists((friend)-[:KNOWS]-(person)) \
        WITH DISTINCT person, friend \
        MATCH (friend)-[:IS_LOCATED_IN]->(city:Place) \
        OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post) \
        WITH person, friend, city.name AS personCityName, post, \
        CASE WHEN exists((post)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)) THEN 1 ELSE 0 END AS common \
        WITH friend, personCityName, count(distinct id(post)) AS postCount, sum(common) AS commonPostCount \
        RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, \
        commonPostCount - (postCount - commonPostCount) AS commonInterestScore, \
        friend.gender AS personGender, personCityName \
        ORDER BY commonInterestScore DESC, personId ASC LIMIT 10;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_11(self, param):
        personId = param[0]
        countryName = param[1]
        workFromYear = param[2]
        query = "MATCH (person:Person{id:"+personId+"})-[:KNOWS*1..2]-(friend:Person) \
        WHERE id(person) != id(friend) WITH DISTINCT friend \
        MATCH (friend)-[worksAt:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(:Place {name:"+countryName+"}) \
        WHERE worksAt.workFrom < "+workFromYear+" \
        RETURN friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName, \
        company.name AS companyName, worksAt.workFrom AS workFromYear \
        ORDER BY workFromYear ASC, friendId ASC, companyName DESC LIMIT 10;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_12(self, param):
        personId = param[0]
        tagClassName = param[1]
        query = "MATCH (person:Person{id:"+personId+"})-[:KNOWS]-(friend:Person) \
        OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag), \
        (tag)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass) \
        WHERE tagClass.name = "+tagClassName+" OR baseTagClass.name = "+tagClassName+" \
        RETURN friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName, \
        collect(DISTINCT tag.name) AS tagNames, count(DISTINCT id(c)) AS count ORDER BY count DESC, friendId ASC LIMIT 20;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_13(self, param):
        personId = param[0]
        personId2 = param[1]
        query = "MATCH (person1:Person), (person2:Person) \
        WHERE person1.id = "+personId+" AND person2.id = "+personId2+" \
        OPTIONAL MATCH path = shortestpath((person1)-[:KNOWS*..15]-(person2)) \
        RETURN CASE path IS NULL WHEN true THEN -1 ELSE length(path) END AS pathLength;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def i_complex_14(self, param, c14_weight, c14_func):
        personId = param[0]
        personId2 = param[1]
        query = "MATCH (person1:Person), (person2:Person) \
        WHERE person1.id = "+personId+" AND person2.id = "+personId2+" \
        OPTIONAL MATCH path = allshortestpaths( (person1)-[:KNOWS*..15]-(person2) ) \
        RETURN extract_ids2(nodes(path)) AS pathNodes, \
        get_weight2(nodes(path)) AS weight ORDER BY weight DESC;"
        self.cur.execute(c14_weight) # create UDF
        self.cur.execute(c14_func)
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_1(self, param):
        date = param[0]
        query = "MATCH (message:Message) WHERE message.creationDate < " + date + " \
        WITH count(message) AS totalMessageCountInt \
        WITH (1.0*totalMessageCountInt) AS totalMessageCount \
        MATCH (message:Message) WHERE message.creationDate < " + date + " AND message.content IS NOT NULL \
        WITH totalMessageCount, message, message.creationDate/10000000000000 AS year \
        WITH totalMessageCount, year, (label(message) = 'comment') AS isComment, \
        CASE WHEN message.length < 40 THEN 0 WHEN message.length < 80 THEN 1 WHEN message.length < 160 THEN 2 ELSE 3 END AS lengthCategory, \
        count(message) AS messageCount, avg(message.length) AS averageMessageLength, sum(message.length) AS sumMessageLength \
        RETURN year, isComment, lengthCategory, messageCount, floor(averageMessageLength) AS averageMessageLength, \
        sumMessageLength, messageCount / totalMessageCount AS percentageOfMessages \
        ORDER BY year DESC, isComment ASC, lengthCategory ASC;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_2(self, param):
        startDate = param[0]
        endDate = param[1]
        country1Name = param[2]
        country2Name = param[3]
        query = "MATCH (country:Place)<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(tag:Tag) \
        WHERE message.creationDate >= " + startDate + " AND message.creationDate <= " + endDate + " \
        AND (country.name = " + country1Name + " OR country.name = " + country2Name + ") \
        WITH country.name AS countryName, message.creationDate/100000000000%100 AS month, \
        person.gender AS gender, floor((20130101 - person.birthday) / 10000 / 5.0) AS ageGroup, tag.name AS tagName, message \
        WITH countryName, month, gender, ageGroup, tagName, count(message) AS messageCount WHERE messageCount > 100 \
        RETURN countryName, month, gender, ageGroup, tagName, messageCount \
        ORDER BY messageCount DESC, tagName ASC, ageGroup ASC, gender ASC, month ASC, countryName ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_3(self, param):
        year = param[0]
        month = param[1]
        query = "MATCH (tag:Tag) \
        OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag) \
        WHERE message1.creationDate/10000000000000   = " + year + " AND message1.creationDate/100000000000%100 = " + month + " \
        WITH (" + year + " + " + month + " / 12) AS year2, (" + month + " % 12 + 1) AS month2, tag, count(message1) AS countMonth1 \
        OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag) \
        WHERE message2.creationDate/10000000000000   = year2 AND message2.creationDate/100000000000%100 = month2 \
        WITH tag, countMonth1, count(message2) AS countMonth2 \
        RETURN tag.name AS tagName, countMonth1, countMonth2, abs(countMonth1-countMonth2) AS diff \
        ORDER BY diff DESC, tagName ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_4(self, param):
        country = param[1]
        tagClass = param[0]
        query = "MATCH (:Place {name: " + country + "})<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_MODERATOR]\
        -(forum:Forum)-[:CONTAINER_OF]->(post:Post)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: " + tagClass + "}) \
        RETURN forum.id as forumId, forum.title, forum.creationDate, person.id, count(DISTINCT post) AS postCount \
        ORDER BY postCount DESC, forumId ASC LIMIT 20;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_5(self, param):
        country = param[0]
        query = "MATCH (:Place {name: "+country+"})<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_MEMBER]-(forum:Forum) \
        WITH forum, count(person) AS numberOfMembers \
        ORDER BY numberOfMembers DESC, forum.id ASC LIMIT 100 \
        WITH collect(forum.id) AS popularForums \
        MATCH (forum:Forum)-[:HAS_MEMBER]->(person:Person) WHERE forum.id IN popularForums \
        OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(popularForum:Forum) WHERE popularForum.id IN popularForums \
        RETURN person.id AS personId, person.firstName, person.lastName, person.creationDate, count(DISTINCT post) AS postCount \
        ORDER BY postCount DESC, personId ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_6(self, param):
        tag = param[0]
        query = "MATCH (tag:Tag {name: " + tag + "})<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person) \
        OPTIONAL MATCH (:Person)-[li:LIKES]->(message) \
        OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Comment) \
        WITH person, count(DISTINCT li) AS likeCount, count(DISTINCT comment) AS replyCount, count(DISTINCT message) AS messageCount \
        RETURN person.id as personId, replyCount, likeCount, messageCount, 1*messageCount + 2*replyCount + 10*likeCount AS score \
        ORDER BY score DESC, personId ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_7(self, param):
        tag = param[0]
        query = "MATCH (tag:Tag {name: " + tag + "}) \
        MATCH (tag)<-[:HAS_TAG]-(message1:Message)-[:HAS_CREATOR]->(person1:Person) \
        MATCH (tag)<-[:HAS_TAG]-(message2:Message)-[:HAS_CREATOR]->(person1) \
        OPTIONAL MATCH (message2)<-[:LIKES]-(person2:Person) \
        OPTIONAL MATCH (person2)<-[:HAS_CREATOR]-(message3:Message)<-[li:LIKES]-(p3:Person) \
        RETURN person1.id AS personId, count(DISTINCT li) AS authorityScore \
        ORDER BY authorityScore DESC, personId ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_8(self, param):
        tag = param[0]
        query = "MATCH (tag:Tag {name: " + tag + "})<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag) \
        WHERE NOT EXISTS((comment)-[:HAS_TAG]->(tag)) \
        RETURN relatedTag.name as relatedTagName, count(DISTINCT comment) AS count \
        ORDER BY count DESC, relatedTagName ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_9(self, param):
        tagClass1 = param[0]
        tagClass2 = param[1]
        threshold = param[2]
        query = "MATCH (forum:Forum)-[:HAS_MEMBER]->(person:Person) \
        WITH forum, count(person) AS members WHERE members > " + threshold + " \
        MATCH (forum)-[:CONTAINER_OF]->(post1:Post)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: " + tagClass1 + "}) \
        WITH forum, count(DISTINCT post1) AS count1 \
        MATCH (forum)-[:CONTAINER_OF]->(post2:Post)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: " + tagClass2 +"}) \
        WITH forum, count1, count(DISTINCT post2) AS count2 \
        RETURN forum.id as forumId, count1, count2 \
        ORDER BY abs(count2-count1) DESC, forumId ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_10(self, param):
        tag = param[0]
        date = param[1]
        query = "MATCH (tag:Tag {name: "+tag+"}) \
        OPTIONAL MATCH (tag)<-[interest:HAS_INTEREST]-(person:Person) \
        WITH tag, collect(person.id) AS interestedPersons \
        OPTIONAL MATCH (tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person) WHERE message.creationDate > "+date+" \
        WITH tag, interestedPersons, collect(person.id) AS persons \
        WITH collect(DISTINCT tag.id) AS tags, interestedPersons + persons AS persons \
        MATCH (person:Person),(tag:Tag) WHERE person.id IN persons AND tag.id IN tags \
        OPTIONAL MATCH (tag)<-[interest:HAS_INTEREST]-(person) \
        WITH tag, person, CASE WHEN interest IS NULL THEN 0 ELSE 1 END AS interestScore \
        OPTIONAL MATCH (tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person) WHERE message.creationDate > "+date+" \
        WITH tag, person, interestScore, CASE WHEN message IS NULL THEN 0 ELSE 1 END AS messageScore \
        WITH tag, person, sum(interestScore) AS interestScore, sum(messageScore) AS messageScore \
        WITH tag, person, 100 * interestScore + messageScore AS score \
        OPTIONAL MATCH (person)-[:KNOWS]-(friend) \
        WITH tag, person, friend, score \
        OPTIONAL MATCH (tag)<-[interest:HAS_INTEREST]-(friend) \
        WITH tag, person, friend, score, CASE WHEN interest IS NULL THEN 0 ELSE 1 END AS interestScore \
        OPTIONAL MATCH (tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(friend) WHERE message.creationDate > "+date+" \
        WITH tag, person, score, friend, interestScore, CASE WHEN message IS NULL THEN 0 ELSE 1 END AS messageScore \
        WITH tag, person, score, sum(interestScore) AS interestScore, sum(messageScore) AS messageScore \
        WITH tag, person, score, 100 * interestScore + messageScore AS friendScore \
        RETURN person.id AS personId, score, friendScore AS friendsScore \
        ORDER BY score + friendsScore DESC, personId ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_11(self, param):
        country = param[0]
        blackList = param[1]
        query = "MATCH (country:Place {name: "+country+"})<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-\
        (person:Person)<-[:HAS_CREATOR]-(reply:Comment)-[:REPLY_OF]->(message:Message), \
        (reply)-[:HAS_TAG]->(tag:Tag) \
        WHERE NOT EXISTS((message)-[:HAS_TAG]->(:Tag)<-[:HAS_TAG]-(reply)) \
        WITH person, tag, reply, "+blackList+" AS blacklist \
        WITH person, tag, reply, [word IN blacklist WHERE reply.content CONTAINS word | word] AS containWords \
        WHERE length(containWords) = 0 \
        OPTIONAL MATCH (:Person)-[li:LIKES]->(reply) \
        RETURN person.id AS personId, tag.name AS tagName, count(DISTINCT li) AS countLikes, count(DISTINCT reply) AS countReplies \
        ORDER BY countLikes DESC, personId ASC, tagName ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_12(self, param):
        date = param[0]
        likeThreshold = param[1]
        query = "MATCH (creator:Person)<-[:HAS_CREATOR]-(message:Message)<-[li:LIKES]-(:Person) \
        WHERE message.creationDate > " + date + " \
        WITH message, creator, count(li) AS likeCount WHERE likeCount > " + likeThreshold + " \
        RETURN message.id AS messageId, message.creationDate, creator.firstName, creator.lastName, likeCount \
        ORDER BY likeCount DESC, messageId ASC LIMIT 100"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_13(self, param):
        country = param[0]
        query = "SELECT R.year, R.month, R.tagName, R.popularity FROM \
        (SELECT T.year AS year, T.month AS month, T.tagName AS tagName, T.popularity AS popularity, \
        Row_Number() OVER (partition by T.year, T.month ORDER BY T.popularity desc, T.tagName) rank FROM \
        (MATCH (p:Place {name: "+country+"})<-[:IS_LOCATED_IN]-(message:Message) \
        WHERE p.label = 'Country' \
        OPTIONAL MATCH (message)-[:HAS_TAG]->(tag:Tag) \
        WITH message.creationDate/10000000000000 AS year, message.creationDate/100000000000%100 AS month, message, tag \
        WITH year, month, count(message) AS popularity, tag WHERE tag.name IS NOT NULL \
        RETURN year, month, tag.name AS tagName, popularity) AS T) AS R \
        WHERE R.rank <= 5 ORDER BY R.year DESC, R.month ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_14(self, param):
        startDate = param[0]
        endDate = param[1]
        query = "MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF*0..]-(reply:Message) \
        WHERE post.creationDate >= " + startDate + " AND post.creationDate <= " + endDate + " \
        AND reply.creationDate >= " + startDate + " AND reply.creationDate <= " + endDate + " \
        RETURN person.id AS personId, person.firstName, person.lastName, count(DISTINCT post) AS threadCount, count(DISTINCT reply) AS messageCount \
        ORDER BY messageCount DESC, personId ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_15(self, param):
        country = param[0]
        query = "MATCH (country:Place {name: " + country + "}) \
        MATCH (country)<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(person1:Person) \
        OPTIONAL MATCH (country)<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(friend1:Person)-[:KNOWS]-(person1) \
        WITH country, person1, count(friend1) AS friend1Count \
        WITH country, avg(friend1Count) AS socialNormalFloat \
        WITH country, floor(socialNormalFloat) AS socialNormal \
        MATCH (country)<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(person2:Person) \
        OPTIONAL MATCH (country)<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(friend2:Person)-[:KNOWS]-(person2) \
        WITH country, person2, count(friend2) AS friend2Count, socialNormal \
        WHERE friend2Count = socialNormal \
        RETURN person2.id AS person2Id, friend2Count AS count \
        ORDER BY person2Id ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_16(self, param):
        personId = param[0]
        country = param[1]
        tagClass = param[2]
        minPathDistance = param[3]
        maxPathDistance = param[4]
        query = "MATCH (:Person {id: " + personId + "})-[:KNOWS*" + minPathDistance + ".." + maxPathDistance + "]-(person:Person) \
        WITH DISTINCT person \
        MATCH (:Place {name: " + country + "})<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(person)<-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: " + tagClass + "}) \
        MATCH (message)-[:HAS_TAG]->(tag:Tag) \
        RETURN person.id AS personId, tag.name AS tagName, count(DISTINCT message) AS messageCount \
        ORDER BY messageCount DESC, tagName ASC, personId ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_17(self, param):
        country = param[0]
        query = "MATCH (country:Place {name: " + country + "}) \
        MATCH (a:Person)-[:IS_LOCATED_IN]->(:Place)-[:IS_PART_OF]->(country) \
        MATCH (b:Person)-[:IS_LOCATED_IN]->(:Place)-[:IS_PART_OF]->(country) \
        MATCH (c:Person)-[:IS_LOCATED_IN]->(:Place)-[:IS_PART_OF]->(country) \
        MATCH (a)-[:KNOWS]-(b), (b)-[:KNOWS]-(c), (c)-[:KNOWS]-(a) \
        WHERE a.id < b.id AND b.id < c.id RETURN count(*) AS count;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_18(self, param):
        date = param[0]
        lengthThreshold = param[1]
        languages = param[2]
        query = "MATCH (person:Person) \
        OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message)-[:REPLY_OF*0..]->(post:Post) \
        WHERE message.content IS NOT NULL AND message.length < " + lengthThreshold + " \
        AND message.creationDate > " + date + " AND post.languages IN " + languages + " \
        WITH person, count(message) AS messageCount \
        WITH count(person) AS personCount, messageCount \
        RETURN messageCount, personCount \
        ORDER BY personCount DESC, messageCount DESC;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_19(self, param):
        date = param[0]
        tagClass1 = param[1]
        tagClass2 = param[2]
        query = "MATCH (:TagClass {name: "+tagClass1+"})<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(forum1:Forum)-[:HAS_MEMBER]->(stranger:Person) \
        WITH DISTINCT stranger \
        MATCH (:TagClass {name: "+tagClass2+"})<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(forum2:Forum)-[:HAS_MEMBER]->(stranger) \
        WITH DISTINCT stranger \
        MATCH (person:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF*]->(message:Message)-[:HAS_CREATOR]->(stranger) \
        WHERE person.birthday > "+date+" \
        AND person <> stranger AND NOT EXISTS((person)-[:KNOWS]-(stranger)) \
        AND NOT EXISTS((message)-[:REPLY_OF*]->()-[:HAS_CREATOR]->(stranger)) \
        RETURN person.id AS personId, count(DISTINCT stranger) AS strangersCount, count(comment) AS interactionCount \
        ORDER BY interactionCount DESC, personId ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_20(self, param):
        tagClasses = param[0]
        query = "UNWIND "+tagClasses+" AS tagClassName \
        MATCH (tagClass:TagClass {name: tagClassName})<-[:IS_SUBCLASS_OF*0..]-(:TagClass)<-[:HAS_TYPE]-(tag:Tag)<-[:HAS_TAG]-(message:Message) \
        RETURN tagClass.name AS tagclassName, count(DISTINCT message) AS messageCount \
        ORDER BY messageCount DESC, tagclassName ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_21(self, param):
        country = param[0]
        endDate = param[1]
        query = "MATCH (country:Place {name: "+country+"}) \
        WITH country, "+endDate+"/10000000000000 AS endDateYear, "+endDate+"/100000000000%100 AS endDateMonth \
        MATCH (country)<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(zombie:Person)<-[:HAS_CREATOR]-(message:Message) \
        WHERE zombie.creationDate  < "+endDate+" AND message.creationDate < "+endDate+" \
        WITH country, zombie, endDateYear, endDateMonth, zombie.creationDate/10000000000000 AS zombieCreationYear, \
        zombie.creationDate/100000000000%100 AS zombieCreationMonth, count(message) AS messageCount \
        WITH country, zombie, \
        12 * (endDateYear - zombieCreationYear ) + (endDateMonth - zombieCreationMonth) + 1 AS months, messageCount \
        WHERE messageCount / float4(months) <1 \
        WITH country, collect(zombie.id) AS zombies \
        MATCH (zombie:Person) WHERE zombie.id IN zombies \
        OPTIONAL MATCH (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerZombie:Person) \
        WHERE likerZombie.id IN zombies \
        WITH zombie, count(likerZombie) AS zombieLikeCount \
        OPTIONAL MATCH (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerPerson:Person) \
        WHERE likerPerson.creationDate < "+endDate+" \
        WITH zombie, zombieLikeCount, count(likerPerson) AS totalLikeCount \
        RETURN zombie.id AS zombieId, zombieLikeCount, totalLikeCount AS totalLikeCount, \
        CASE totalLikeCount = 0 WHEN true THEN 0.0 ELSE zombieLikeCount / float4(totalLikeCount) END AS zombieScore \
        ORDER BY zombieScore DESC, zombieId ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_22(self, param):
        country1 = param[0]
        country2 = param[1]
        query = "SELECT R.person1Id, R.person2Id, R.cityName, R.topScore FROM \
        (SELECT T.city1Name AS cityName,T.tp1id AS person1Id, T.tp2id AS person2Id, T.topScore AS topScore, \
        Row_Number() OVER (partition by T.city1Name ORDER BY T.topScore desc, T.tp1id, T.tp2id) rank FROM \
        (MATCH (country1:Place {name: "+country1+"})<-[:IS_PART_OF]-(city1:Place)<-[:IS_LOCATED_IN]-(person1:Person), \
        (country2:Place {name: "+country2+"})<-[:IS_PART_OF]-(city2:Place)<-[:IS_LOCATED_IN]-(person2:Person) \
        WITH person1, person2, city1, 0 AS score \
        OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(message:Message)-[:HAS_CREATOR]->(person2) \
        WITH DISTINCT person1, person2, city1, score + (CASE c is null WHEN true THEN 0 ELSE 4 END) AS score \
        OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m)<-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]->(person2) \
        WITH DISTINCT person1, person2, city1, score + (CASE m is null WHEN true THEN 0 ELSE 1 END) AS score \
        OPTIONAL MATCH (person1)-[k:KNOWS]-(person2) \
        WITH DISTINCT person1, person2, city1, score + (CASE k is null WHEN true THEN 0 ELSE 15 END) AS score \
        OPTIONAL MATCH (person1)-[:LIKES]->(m)-[:HAS_CREATOR]->(person2) \
        WITH DISTINCT person1, person2, city1, score + (CASE m is null WHEN true THEN 0 ELSE 10 END) AS score \
        OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m)<-[:LIKES]-(person2) \
        WITH DISTINCT person1, person2, city1, score + (CASE m is null WHEN true THEN 0 ELSE 1 END) AS score \
        RETURN person1.id AS tp1id, person2.id AS tp2id, city1.name AS city1Name, score AS topScore) AS T) AS R \
        WHERE R.rank = 1 ORDER BY R.topScore desc, R.person1Id, R.person2Id LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_23(self, param):
        country = param[0]
        query = "MATCH (home:Place {name: "+country+"})<-[:IS_PART_OF]-(:Place)<-[:IS_LOCATED_IN]-(:Person)<-[:HAS_CREATOR]-(message:Message)-[:IS_LOCATED_IN]->(destination:Place) \
        WHERE home <> destination \
        WITH message, destination, message.creationDate/100000000000%100 AS month \
        RETURN count(message) AS messageCount, destination.name AS desName, month \
        ORDER BY messageCount DESC, desName ASC, month ASC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_24(self, param):
        tagClass = param[0]
        query = "MATCH (:TagClass {name: "+tagClass+"})<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(message:Message) \
        WITH DISTINCT message \
        MATCH (message)-[:IS_LOCATED_IN]->(:Place)-[:IS_PART_OF]->(continent:Place) OPTIONAL MATCH (message)<-[li:LIKES]-(:Person) \
        WITH message, message.creationDate/10000000000000 AS year, message.creationDate/100000000000%100 AS month, li, continent \
        RETURN count(DISTINCT message) AS messageCount, count(li) AS likeCount, year, month, continent.name AS continentName \
        ORDER BY year ASC, month ASC, continentName DESC LIMIT 100;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

    def bi_25(self, param):
        personId = param[0]
        personId2 = param[1]
        startDate = param[2]
        endDate = param[3]
        query = "MATCH path=allShortestPaths((p1:Person {id: "+personId+"})-[:KNOWS*]-(p2:Person {id: "+personId2+"})) \
        UNWIND relationships(path) AS k \
        WITH path, startNode(k) AS pA, endNode(k) AS pB, 0 AS relationshipWeights \
        OPTIONAL MATCH (pA)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(pB), \
        (post)<-[:CONTAINER_OF]-(forum:Forum) \
        WHERE forum.creationDate >= "+startDate+" AND forum.creationDate <= "+endDate+" \
        WITH path, pA, pB, relationshipWeights, count(c) AS commentWeights \
        WITH path, pA, pB, relationshipWeights + commentWeights*1.0 AS relationshipWeights \
        OPTIONAL MATCH (pA)<-[:HAS_CREATOR]-(c1:Comment)-[:REPLY_OF]->(c2:Comment)-[:HAS_CREATOR]->(pB), \
        (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum) \
        WHERE forum.creationDate >= "+startDate+" AND forum.creationDate <= "+endDate+" \
        WITH path, pA, pB, relationshipWeights, count(c1) AS commentWeights \
        WITH path, pA, pB, relationshipWeights + commentWeights*0.5 AS relationshipWeights \
        OPTIONAL MATCH (pB)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(pA), \
        (post)<-[:CONTAINER_OF]-(forum:Forum) \
        WHERE forum.creationDate >= "+startDate+" AND forum.creationDate <= "+endDate+" \
        WITH path, pA, pB, relationshipWeights, count(c) AS commentWeights \
        WITH path, pA, pB, relationshipWeights + commentWeights*1.0 AS relationshipWeights \
        OPTIONAL MATCH (pB)<-[:HAS_CREATOR]-(c1:Comment)-[:REPLY_OF]->(c2:Comment)-[:HAS_CREATOR]->(pA), \
        (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum) \
        WHERE forum.creationDate >= "+startDate+" AND forum.creationDate <= "+endDate+" \
        WITH path, pA, pB, relationshipWeights, count(c1) AS commentWeights \
        WITH path, pA, pB, relationshipWeights + commentWeights*0.5 AS relationshipWeights \
        WITH extract_ids2(nodes(path)) AS personIds, sum(relationshipWeights) AS weight \
        RETURN personIds, weight ORDER BY weight DESC, personIds ASC;"
        self.cur.execute(query)
        result = self.cur.fetchall()
        print result
        return result

if __name__ == "__main__":
    runner = AgensQueryRunner() 
    runner.i_short_2(['17592186053137'])
