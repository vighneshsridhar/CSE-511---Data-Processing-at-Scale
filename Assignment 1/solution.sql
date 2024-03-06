CREATE TABLE users
	(userid INTEGER PRIMARY KEY,
	name TEXT
	);

CREATE TABLE movies
	(movieid INTEGER PRIMARY KEY,
	title TEXT
	);

CREATE TABLE taginfo
	(tagid INTEGER PRIMARY KEY,
	content TEXT
	);

CREATE TABLE genres
	(genreid INTEGER PRIMARY KEY,
	name TEXT
	);

CREATE TABLE ratings
	(userid INTEGER,
	movieid INTEGER,
	rating NUMERIC CHECK (rating >= 0 AND rating <= 5),
	timestamp BIGINT,
	FOREIGN KEY (userid) REFERENCES users(userid),
	FOREIGN KEY (movieid) REFERENCES movies(movieid),
	CONSTRAINT mr_ratings PRIMARY KEY (userid, movieid)
	);


CREATE TABLE tags
	(userid INTEGER,
	movieid INTEGER,
	tagid INTEGER,
	timestamp BIGINT,
	FOREIGN KEY (userid) REFERENCES users(userid),
	FOREIGN KEY (movieid) REFERENCES movies(movieid),
	FOREIGN KEY (tagid) REFERENCES taginfo(tagid)
	);

CREATE TABLE hasagenre
	(movieid INTEGER,
	genreid INTEGER,
	FOREIGN KEY (movieid) REFERENCES movies(movieid),
	FOREIGN KEY (genreid) REFERENCES genres(genreid)
	);