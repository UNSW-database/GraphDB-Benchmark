-- pre_eval weights
set graph_path = ldbc_graph;
DROP TABLE IF EXISTS c14_weight;
CREATE UNLOGGED TABLE c14_weight(p1 bigint, p2 bigint, weight double precision);
INSERT INTO c14_weight
    SELECT (p1->>0)::bigint, (p2->>0)::bigint, SUM((inc->>0)::float) FROM (
        SELECT
            rep_creator AS p1,
            org_creator AS p2,
            inc
        FROM
		(
            MATCH
            (p1:Person)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(m:Post)-[:HAS_CREATOR]->(p2:Person)
			, (p1:Person)-[:KNOWS]->(p2:Person)
			RETURN p1.id AS rep_creator, p2.id AS org_creator, 1.0 AS inc
			UNION ALL
			MATCH
            (p1:Person)<-[:HAS_CREATOR]-(m:Post)<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p2:Person)
			, (p1:Person)-[:KNOWS]->(p2:Person)
			RETURN p1.id AS rep_creator, p2.id AS org_creator, 1.0 AS inc
            UNION ALL
            MATCH
            (p1:Person)-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]-(m:Comment)-[:HAS_CREATOR]-(p2:Person)
			, (p1:Person)-[:KNOWS]->(p2:Person)
			RETURN p1.id AS rep_creator, p2.id AS org_creator, 0.5 AS inc
        ) AS x
    ) AS x
    GROUP BY p1, p2;
CREATE UNIQUE INDEX ON c14_weight(p1, p2);
ALTER TABLE c14_weight SET LOGGED;
