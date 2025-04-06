// Part A

MATCH (f:Film)
WITH count(f) AS filmCount
MATCH (p:Planet)
WITH filmCount, count(p) AS planetCount
MATCH (s:Species)
RETURN filmCount, planetCount, count(s) AS speciesCount;

// Part B

MATCH (f:Film {title: "A New Hope"})-[:FEATURES_PLANET]->(p:Planet)
RETURN p.name;

// Part C

MATCH (f:Film)
WHERE date(f.release_date) > date("1980-12-31") AND f.imdb_rating >= 5
RETURN f.title, f.release_date, f.imdb_rating

// Part D

MATCH (f:Film)-[:FEATURES_VEHICLE]->(v:Vehicle)
WHERE v.name IN ["Sand Crawler", "X-34 landspeeder"]
RETURN DISTINCT f.title, collect(v.name) AS vehicles;

// Part E

MATCH (f:Film)
RETURN f.title, size(f.keywords) AS keywordCount
ORDER BY keywordCount DESC
LIMIT 1;

// Part F

CALL db.index.fulltext.createNodeIndex(
  "filmOverviewIndex", ["Film"], ["overview"]
);

// Part G

CALL db.index.fulltext.queryNodes("filmOverviewIndex", "rebellion")
YIELD node, score
RETURN node.title AS title, node.overview, score
ORDER BY score DESC;