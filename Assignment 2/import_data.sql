COPY users FROM 'C:\Users\Vighnesh_Laptop\Documents\Arizona State University MCS\Data Processing at Scale\Assignment 2\example_input\users.csv' DELIMITER '%' CSV HEADER;

COPY movies FROM 'C:\Users\Vighnesh_Laptop\Documents\Arizona State University MCS\Data Processing at Scale\Assignment 2\example_input\movies.csv' DELIMITER '%' CSV HEADER;

COPY taginfo FROM 'C:\Users\Vighnesh_Laptop\Documents\Arizona State University MCS\Data Processing at Scale\Assignment 2\example_input\taginfo.csv' DELIMITER '%' CSV HEADER;

COPY genres FROM 'C:\Users\Vighnesh_Laptop\Documents\Arizona State University MCS\Data Processing at Scale\Assignment 2\example_input\genres.csv' DELIMITER '%' CSV HEADER;

COPY ratings FROM 'C:\Users\Vighnesh_Laptop\Documents\Arizona State University MCS\Data Processing at Scale\Assignment 2\example_input\ratings.csv' DELIMITER '%' CSV HEADER;

COPY tags FROM 'C:\Users\Vighnesh_Laptop\Documents\Arizona State University MCS\Data Processing at Scale\Assignment 2\example_input\tags.csv' DELIMITER '%' CSV HEADER;

COPY hasagenre FROM 'C:\Users\Vighnesh_Laptop\Documents\Arizona State University MCS\Data Processing at Scale\Assignment 2\example_input\hasagenre.csv' DELIMITER '%' CSV HEADER;