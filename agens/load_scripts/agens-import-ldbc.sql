\set nw_comment :cwd comment_0_0.csv
\set nw_forum :cwd forum_0_0.csv
\set nw_organisation :cwd organisation_0_0.csv
\set nw_person :cwd person_0_0.csv
\set nw_place :cwd place_0_0.csv
\set nw_post :cwd post_0_0.csv
\set nw_tag :cwd tag_0_0.csv
\set nw_tagclass :cwd tagclass_0_0.csv
\set nw_comment_hascreator_person :cwd comment_hasCreator_person_0_0.csv
\set nw_comment_hastag_tag :cwd comment_hasTag_tag_0_0.csv
\set nw_comment_islocatedin_place :cwd comment_isLocatedIn_place_0_0.csv
\set nw_comment_replyof_comment :cwd comment_replyOf_comment_0_0.csv
\set nw_comment_replyof_post :cwd comment_replyOf_post_0_0.csv
\set nw_forum_containerof_post :cwd forum_containerOf_post_0_0.csv
\set nw_forum_hasmember_person :cwd forum_hasMember_person_0_0.csv
\set nw_forum_hasmoderator_person :cwd forum_hasModerator_person_0_0.csv
\set nw_forum_hastag_tag :cwd forum_hasTag_tag_0_0.csv
\set nw_organisation_islocatedin_place :cwd organisation_isLocatedIn_place_0_0.csv
\set nw_person_hasinterest_tag :cwd person_hasInterest_tag_0_0.csv
\set nw_person_islocatedin_place :cwd person_isLocatedIn_place_0_0.csv
\set nw_person_knows_person :cwd person_knows_person_0_0.csv
\set nw_person_likes_comment :cwd person_likes_comment_0_0.csv
\set nw_person_likes_post :cwd person_likes_post_0_0.csv
\set nw_person_studyat_organisation :cwd person_studyAt_organisation_0_0.csv
\set nw_person_workat_organisation :cwd person_workAt_organisation_0_0.csv
\set nw_place_ispartof_place :cwd place_isPartOf_place_0_0.csv
\set nw_post_hascreator_person :cwd post_hasCreator_person_0_0.csv
\set nw_post_hastag_tag :cwd post_hasTag_tag_0_0.csv
\set nw_post_islocatedin_place :cwd post_isLocatedIn_place_0_0.csv
\set nw_tagclass_issubclassof_tagclass :cwd tagclass_isSubclassOf_tagclass_0_0.csv
\set nw_tag_hastype_tagclass :cwd tag_hasType_tagclass_0_0.csv

START TRANSACTION;

CREATE EXTENSION IF NOT EXISTS file_fdw;

CREATE SERVER ldbc FOREIGN DATA WRAPPER file_fdw;

CREATE GRAPH ldbc_graph;
SET graph_path = ldbc_graph;

CREATE VLABEL Forum;
CREATE VLABEL Message;
CREATE VLABEL Post INHERITS (Message);
CREATE VLABEL Comment INHERITS (Message);
CREATE VLABEL Organisation;
CREATE VLABEL Person;
CREATE VLABEL Place;
CREATE VLABEL Tag;
CREATE VLABEL TagClass;

CREATE ELABEL HAS_CREATOR;
CREATE ELABEL IS_LOCATED_IN;
CREATE ELABEL REPLY_OF;
CREATE ELABEL CONTAINER_OF;
CREATE ELABEL HAS_MEMBER;
CREATE ELABEL HAS_MODERATOR;
CREATE ELABEL HAS_TAG;
CREATE ELABEL HAS_INTEREST;
CREATE ELABEL KNOWS;
CREATE ELABEL LIKES;
CREATE ELABEL IS_PART_OF;
CREATE ELABEL IS_SUBCLASS_OF;
CREATE ELABEL HAS_TYPE;
CREATE ELABEL STUDY_AT;
CREATE ELABEL WORK_AT;

CREATE FOREIGN TABLE nw_comment (
    id bigint,
    creationDate bigint,
    locationIP varchar(20),
    browserUsed varchar(50),
    content text,
    length int
)
SERVER ldbc
OPTIONS (
    filename :'nw_comment',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_forum (
    id bigint,
    title varchar(150),
    creationDate bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_forum',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_organisation (
    id bigint,
    label varchar(20),
    name varchar(500),
    url varchar(500)
)
SERVER ldbc
OPTIONS (
    filename :'nw_organisation',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_person (
    id bigint,
    firstName varchar(100),
    lastName varchar(100),
    gender varchar(10),
    birthday bigint,
    creationDate bigint,
    locationIP varchar(20),
    browserUsed varchar(50),
    languages text,
    emails text
)
SERVER ldbc
OPTIONS (
    filename :'nw_person',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_place (
    id bigint,
    name varchar(200),
    url varchar(500),
    label varchar(20)
)
SERVER ldbc
OPTIONS (
    filename :'nw_place',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_post (
    id bigint,
    imageFile varchar(200),
    creationDate bigint,
    locationIP varchar(20),
    browserUsed varchar(50),
    languages varchar(50),
    content text,
    length int
)
SERVER ldbc
OPTIONS (
    filename :'nw_post',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_tag (
    id bigint,
    name varchar(200),
    url varchar(500)
)
SERVER ldbc
OPTIONS (
    filename :'nw_tag',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_tagclass (
    id bigint,
    name varchar(200),
    url varchar(500)
)
SERVER ldbc
OPTIONS (
    filename :'nw_tagclass',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_comment_hascreator_person (
    CommentId bigint,
    PersonId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_comment_hascreator_person',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_comment_hastag_tag (
    CommentId bigint,
    TagId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_comment_hastag_tag',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_comment_islocatedin_place (
    CommentId bigint,
    PlaceId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_comment_islocatedin_place',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_comment_replyof_comment (
    CommentId bigint,
    CommentId2 bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_comment_replyof_comment',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_comment_replyof_post (
    CommentId bigint,
    PostId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_comment_replyof_post',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_forum_containerof_post (
    ForumId bigint,
    PostId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_forum_containerof_post',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_forum_hasmember_person (
    ForumId bigint,
    PersonId bigint,
    joinDate bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_forum_hasmember_person',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_forum_hasmoderator_person (
    ForumId bigint,
    PersonId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_forum_hasmoderator_person',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_forum_hastag_tag (
    ForumId bigint,
    TagId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_forum_hastag_tag',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_organisation_islocatedin_place (
    OrganisationId bigint,
    PlaceId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_organisation_islocatedin_place',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_person_hasinterest_tag (
    PersonId bigint,
    TagId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_person_hasinterest_tag',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_person_islocatedin_place (
    PersonId bigint,
    PlaceId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_person_islocatedin_place',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_person_knows_person (
    PersonId bigint,
    PersonId2 bigint,
    creationDate bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_person_knows_person',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_person_likes_comment (
    PersonId bigint,
    CommentId bigint,
    creationDate bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_person_likes_comment',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_person_likes_post (
    PersonId bigint,
    PostId bigint,
    creationDate bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_person_likes_post',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_person_studyat_organisation (
    PersonId bigint,
    OrganisationId bigint,
    classYear int
)
SERVER ldbc
OPTIONS (
    filename :'nw_person_studyat_organisation',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_person_workat_organisation (
    PersonId bigint,
    OrganisationId bigint,
    workFrom int
)
SERVER ldbc
OPTIONS (
    filename :'nw_person_workat_organisation',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_place_ispartof_place (
    PlaceId bigint,
    PlaceId2 bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_place_ispartof_place',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_post_hascreator_person (
    PostId bigint,
    PersonId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_post_hascreator_person',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_post_hastag_tag (
    PostId bigint,
    TagId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_post_hastag_tag',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_post_islocatedin_place (
    PostId bigint,
    PlaceId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_post_islocatedin_place',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_tagclass_issubclassof_tagclass (
    TagClassId bigint,
    TagClassId2 bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_tagclass_issubclassof_tagclass',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

CREATE FOREIGN TABLE nw_tag_hastype_tagclass (
    TagId bigint,
    TagClassId bigint
)
SERVER ldbc
OPTIONS (
    filename :'nw_tag_hastype_tagclass',
    format 'csv',
    header 'true',
    delimiter '|',
    null ''
);

LOAD FROM nw_comment AS r CREATE (:Comment =to_jsonb(r));
LOAD FROM nw_forum AS r CREATE (:Forum =to_jsonb(r));
LOAD FROM nw_organisation AS r CREATE (:Organisation =to_jsonb(r));
LOAD FROM nw_person AS r CREATE (:Person =to_jsonb(r));
LOAD FROM nw_post AS r CREATE (:Post =to_jsonb(r));
LOAD FROM nw_place AS r CREATE (:Place =to_jsonb(r));
LOAD FROM nw_tagclass AS r CREATE (:TagClass =to_jsonb(r));
LOAD FROM nw_tag AS r CREATE (:Tag =to_jsonb(r));

LOAD FROM nw_comment_hascreator_person AS r
MATCH (c:Comment), (p:Person)
WHERE c.id = to_jsonb(r.CommentId) AND
      p.id = to_jsonb(r.PersonId)
CREATE (c)-[:HAS_CREATOR]->(p);

LOAD FROM nw_comment_hastag_tag AS r
MATCH (c:Comment), (p:Tag)
WHERE c.id = to_jsonb(r.CommentId) AND
      p.id = to_jsonb(r.TagId)
CREATE (c)-[:HAS_TAG]->(p);

LOAD FROM nw_comment_islocatedin_place AS r
MATCH (c:Comment), (p:Place)
WHERE c.id = to_jsonb(r.CommentId) AND
      p.id = to_jsonb(r.PlaceId)
CREATE (c)-[:IS_LOCATED_IN]->(p);

LOAD FROM nw_comment_replyof_comment AS r
MATCH (c:Comment), (p:Comment)
WHERE c.id = to_jsonb(r.CommentId) AND
      p.id = to_jsonb(r.CommentId2)
CREATE (c)-[:REPLY_OF]->(p);

LOAD FROM nw_comment_replyof_post AS r
MATCH (c:Comment), (p:Post)
WHERE c.id = to_jsonb(r.CommentId) AND
      p.id = to_jsonb(r.PostId)
CREATE (c)-[:REPLY_OF]->(p);

LOAD FROM nw_forum_containerof_post AS r
MATCH (c:Forum), (p:Post)
WHERE c.id = to_jsonb(r.ForumId) AND
      p.id = to_jsonb(r.PostId)
CREATE (c)-[:CONTAINER_OF]->(p);

LOAD FROM nw_forum_hasmember_person AS r
MATCH (c:Forum), (p:Person)
WHERE c.id = to_jsonb(r.ForumId) AND
      p.id = to_jsonb(r.PersonId)
CREATE (c)-[:HAS_MEMBER{joinDate: r.joinDate}]->(p);

LOAD FROM nw_forum_hasmoderator_person AS r
MATCH (c:Forum), (p:Person)
WHERE c.id = to_jsonb(r.ForumId) AND
      p.id = to_jsonb(r.PersonId)
CREATE (c)-[:HAS_MODERATOR]->(p);

LOAD FROM nw_forum_hastag_tag AS r
MATCH (c:Forum), (p:Tag)
WHERE c.id = to_jsonb(r.ForumId) AND
      p.id = to_jsonb(r.TagId)
CREATE (c)-[:HAS_TAG]->(p);

LOAD FROM nw_organisation_islocatedin_place AS r
MATCH (c:Organisation), (p:Place)
WHERE c.id = to_jsonb(r.OrganisationId) AND
      p.id = to_jsonb(r.PlaceId)
CREATE (c)-[:IS_LOCATED_IN]->(p);

LOAD FROM nw_person_hasinterest_tag AS r
MATCH (c:Person), (p:Tag)
WHERE c.id = to_jsonb(r.PersonId) AND
      p.id = to_jsonb(r.TagId)
CREATE (c)-[:HAS_INTEREST]->(p);

LOAD FROM nw_person_islocatedin_place AS r
MATCH (c:Person), (p:Place)
WHERE c.id = to_jsonb(r.PersonId) AND
      p.id = to_jsonb(r.PlaceId)
CREATE (c)-[:IS_LOCATED_IN]->(p);

LOAD FROM nw_person_knows_person AS r
MATCH (c:Person), (p:Person)
WHERE c.id = to_jsonb(r.PersonId) AND
      p.id = to_jsonb(r.PersonId2)
CREATE (c)-[:KNOWS{creationDate: r.creationDate}]->(p);

LOAD FROM nw_person_likes_comment AS r
MATCH (c:Person), (p:Comment)
WHERE c.id = to_jsonb(r.PersonId) AND
      p.id = to_jsonb(r.CommentId)
CREATE (c)-[:LIKES{creationDate: r.creationDate}]->(p);

LOAD FROM nw_person_likes_post AS r
MATCH (c:Person), (p:Post)
WHERE c.id = to_jsonb(r.PersonId) AND
      p.id = to_jsonb(r.PostId)
CREATE (c)-[:LIKES{creationDate: r.creationDate}]->(p);

LOAD FROM nw_person_studyat_organisation AS r
MATCH (c:Person), (p:Organisation)
WHERE c.id = to_jsonb(r.PersonId) AND
      p.id = to_jsonb(r.OrganisationId)
CREATE (c)-[:STUDY_AT{classYear: r.classYear}]->(p);

LOAD FROM nw_person_workat_organisation AS r
MATCH (c:Person), (p:Organisation)
WHERE c.id = to_jsonb(r.PersonId) AND
      p.id = to_jsonb(r.OrganisationId)
CREATE (c)-[:WORK_AT{workFrom: r.workFrom}]->(p);

LOAD FROM nw_place_ispartof_place AS r
MATCH (c:Place), (p:Place)
WHERE c.id = to_jsonb(r.PlaceId) AND
      p.id = to_jsonb(r.PlaceId2)
CREATE (c)-[:IS_PART_OF]->(p);

LOAD FROM nw_post_hascreator_person AS r
MATCH (c:Post), (p:Person)
WHERE c.id = to_jsonb(r.PostId) AND
      p.id = to_jsonb(r.PersonId)
CREATE (c)-[:HAS_CREATOR]->(p);

LOAD FROM nw_post_hastag_tag AS r
MATCH (c:Post), (p:Tag)
WHERE c.id = to_jsonb(r.PostId) AND
      p.id = to_jsonb(r.TagId)
CREATE (c)-[:HAS_TAG]->(p);

LOAD FROM nw_post_islocatedin_place AS r
MATCH (c:Post), (p:Place)
WHERE c.id = to_jsonb(r.PostId) AND
      p.id = to_jsonb(r.PlaceId)
CREATE (c)-[:IS_LOCATED_IN]->(p);

LOAD FROM nw_tagclass_issubclassof_tagclass AS r
MATCH (c:TagClass), (p:TagClass)
WHERE c.id = to_jsonb(r.TagClassId) AND
      p.id = to_jsonb(r.TagClassId2)
CREATE (c)-[:IS_SUBCLASS_OF]->(p);

LOAD FROM nw_tag_hastype_tagclass AS r
MATCH (c:Tag), (p:TagClass)
WHERE c.id = to_jsonb(r.TagId) AND
      p.id = to_jsonb(r.TagClassId)
CREATE (c)-[:HAS_TYPE]->(p);

COMMIT;
