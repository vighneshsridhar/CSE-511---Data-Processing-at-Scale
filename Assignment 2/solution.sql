CREATE TABLE query1 AS
SELECT g.name, COUNT(m.movieid) AS moviecount
FROM genres g
JOIN hasagenre h ON g.genreid = h.genreid
JOIN movies m ON h.movieid = m.movieid
GROUP BY g.name;

CREATE TABLE query2 AS
SELECT g.name, AVG(r.rating) AS rating
FROM genres g
JOIN hasagenre h ON g.genreid = h.genreid
JOIN movies m ON h.movieid = m.movieid
JOIN ratings r ON m.movieid = r.movieid
GROUP BY g.name;

CREATE TABLE query3 AS
SELECT m.title, COUNT(r.rating) AS countofratings
FROM movies m
JOIN ratings r ON m.movieid = r.movieid
GROUP BY m.title
HAVING COUNT(r.rating) >= 10;

CREATE TABLE query4 AS
SELECT m.movieid, m.title
FROM movies m
JOIN hasagenre h ON m.movieid = h.movieid
JOIN genres g ON g.genreid = h.genreid
WHERE g.name = 'Comedy'
GROUP BY m.movieid;

CREATE TABLE query5 AS
SELECT m.title, AVG(r.rating) AS average
FROM movies m
JOIN ratings r ON m.movieid = r.movieid
GROUP BY m.title;

CREATE TABLE query6 AS
SELECT AVG(r.rating) AS average
FROM movies m
JOIN hasagenre h ON m.movieid = h.movieid
JOIN genres g ON h.genreid = g.genreid
JOIN ratings r ON m.movieid = r.movieid
WHERE g.name = 'Comedy';

CREATE TABLE query7 AS
SELECT AVG(r.rating) AS average
FROM ratings r
JOIN (SELECT m.movieid
FROM movies m
JOIN hasagenre h ON m.movieid = h.movieid
JOIN genres g ON h.genreid = g.genreid
WHERE g.name IN ('Comedy', 'Romance')
GROUP BY m.movieid
HAVING COUNT(*) = 2) sub
USING (movieid);

CREATE TABLE query8 AS
SELECT AVG(r.rating) AS average
FROM ratings r
JOIN (SELECT m.movieid
FROM movies m
JOIN hasagenre h ON m.movieid = h.movieid
JOIN genres g ON h.genreid = g.genreid
WHERE g.name IN ('Comedy', 'Romance')
GROUP BY m.movieid
HAVING ARRAY_AGG(g.name) = '{Romance}') sub
USING (movieid);

CREATE TABLE query9 AS
SELECT movieid, r.rating AS rating
FROM ratings r
WHERE r.userid = :v1;