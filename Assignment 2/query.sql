CREATE TABLE query1 AS
SELECT g.name, COUNT(m.movieid) AS moviecount
FROM genres g
JOIN hasagenre h ON g.genreid = h.genreid
JOIN movies m ON h.movieid = m.movieid
GROUP BY g.name;
