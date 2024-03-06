#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2

#def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()
    create_table = '''CREATE TABLE IF NOT EXISTS {0}(
        userid INT,
        movieid INT,
        rating FLOAT,
        timestamp INT
    );'''


    cursor.execute(create_table.format(ratingstablename))
    drop_query = 'ALTER TABLE {0} DROP timestamp'
    cursor.execute(drop_query.format(ratingstablename))

    with open(ratingsfilepath, 'r') as file:

        for line in file:
            values = line.strip().split("::")
            userid, movieid, rating = int(values[0]), int(values[1]), float(values[2])
            insert_query = 'INSERT INTO {0} (userid, movieid, rating) VALUES ({1}, {2}, {3})'
            cursor.execute(insert_query.format(ratingstablename, userid, movieid, rating))

        #openconnection.commit()
        #cursor.close()
        #openconnection.close()

    pass


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor();
    cursor.execute('SELECT MIN(rating), MAX(rating) FROM {0}'.format(ratingstablename))
    min_ratings, max_ratings = cursor.fetchone()
    range_width = (max_ratings - min_ratings)/numberofpartitions

    for i in range(numberofpartitions):
        lower_bound = i * range_width
        upper_bound = (i + 1) * range_width
        partition_name = 'range_part{0}'.format(i)
        #print ("partition name")
        #print(partition_name)

        if (i == 0):
            partition_query = '''
            CREATE TABLE {0} AS
            SELECT *
            FROM {1}
            WHERE rating >= {2} AND rating <= {3}'''
            cursor.execute(partition_query.format(partition_name, ratingstablename, lower_bound, upper_bound))
        else:
            partition_query = '''
            CREATE TABLE {0} AS
            SELECT *
            FROM {1}
            WHERE rating > {2} AND rating <= {3}'''
            cursor.execute(partition_query.format(partition_name, ratingstablename, lower_bound, upper_bound))

            create_index_query = 'CREATE INDEX {0}_rating_idx ON {1} (Rating);'
            cursor.execute(create_index_query.format(partition_name, partition_name))

    #openconnection.commit()

    pass

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    cursor.execute('SELECT COUNT(*) FROM {0}'.format(ratingstablename))
    num_rows = cursor.fetchone()[0];
    rows_per_partition = num_rows/numberofpartitions

    for partition_index in range(numberofpartitions):
        partition_offset = partition_index * rows_per_partition
        partition_name = 'rrobin_part{0}'.format(partition_index)
        cursor.execute('CREATE TABLE {0} (LIKE {1})'.format(partition_name, ratingstablename))

        insert_query = 'INSERT INTO {0} SELECT * FROM {1} OFFSET {2} LIMIT {3}'
        cursor.execute(insert_query.format(partition_name, ratingstablename, partition_offset, rows_per_partition))


    #openconnection.commit()

    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    openconnection.rollback()

    cursor.execute('''SELECT * FROM information_schema.tables WHERE table_name LIKE 'rrobin_part{0}' '''.format('%'))
    partitions = cursor.fetchall()
    #print (partitions)
    num_partitions = len(partitions)

    if (num_partitions == 0):
        partition_name = "rrobin_part0"
        cursor.execute('CREATE TABLE {0} (LIKE {1})'.format(partition_name, ratingstablename))
        num_partitions += 1

    else:
        #print ("partitions[0][0] = ", partitions[0][1])
        #cursor.execute(f"SELECT COUNT(*) FROM {partitions[0][2]}")
        cursor.execute('SELECT COUNT(*) FROM {0}'.format(ratingstablename))
        count = cursor.fetchone()[0]
        #print ("count = ", count)
        partition_index = count % num_partitions
        partition_name = 'rrobin_part{0}'.format(partition_index)
    #print ("user id = , movieid = , rating = ", userid, itemid, rating)
    #print ("partition name ", partition_name)
    #print ("count = ", count)
    #print ("num partitions = ", num_partitions)
    insert_query = 'INSERT INTO {0} (userid, movieid, rating) VALUES ({1}, {2}, {3})'
    cursor.execute(insert_query.format(partition_name, userid, itemid, rating))
    query = 'SELECT * FROM {0}'
    cursor.execute(query.format(partition_name))
    table = cursor.fetchall()
    #print ("table")

    openconnection.commit()

    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()

    cursor.execute('''SELECT * FROM information_schema.tables WHERE table_name LIKE 'range_part{0}' '''.format('%'))
    partitions = cursor.fetchall()
    #print (partitions)
    num_partitions = len(partitions)
    range_width = 5/num_partitions

    for i in range(num_partitions):
        lower_bound = i * range_width
        upper_bound = (i + 1) * range_width
        if (i == 0):
            if (rating >= lower_bound and rating <= upper_bound):
                partition_name = 'range_part0'
                insert_query = 'INSERT INTO {0} (userid, movieid, rating) VALUES ({1}, {2}, {3})'
                cursor.execute(insert_query.format(partition_name, userid, itemid, rating))
        else:
            if (rating > lower_bound and rating <= upper_bound):
                partition_name = 'range_part{0}'.format(i)
                insert_query = 'INSERT INTO {0} (userid, movieid, rating) VALUES ({1}, {2}, {3})'
                cursor.execute(insert_query.format(partition_name, userid, itemid, rating))



    pass

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    #con = getOpenConnection(dbname='postgres')
    con = getOpenConnection(dbname='dds_assignment')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
