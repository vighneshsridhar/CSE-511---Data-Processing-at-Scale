#!/usr/bin/python3
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    cursor = openconnection.cursor()

    #cursor.execute('''SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'range_part{0}' '''.format('%'))
    cursor.execute('''SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rangeratingspart{0}' '''.format('%'))
    range_partitions = cursor.fetchall()
    #cursor.execute('''SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rrobin_part{0}' '''.format('%'))
    cursor.execute('''SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'roundrobinratingspart{0}' '''.format('%'))
    rrobin_partitions = cursor.fetchall()
    rows = ()

    for r, partition in enumerate(range_partitions):
        #print (range_partitions)
        #print(partition)

        partition_name = ''.join(partition)
        #print(partition_name)
        rating_query = 'SELECT * FROM {0} WHERE rating >= {1} AND rating <= {2}'
        cursor.execute(rating_query.format(partition_name, ratingMinValue, ratingMaxValue))
        ID_ratings = cursor.fetchall()


        for ID_rating in ID_ratings:

            for s, rr_partition in enumerate(rrobin_partitions):
                #print (ID_rating)
                table_range_query = "SELECT 'rangeratingspart{0}', {1}, {2}, {3}"
                table_rr_query = "SELECT 'roundrobinratingspart{0}', {1}, {2}, {3}"
                rr_partition_name = ''.join(rr_partition)
                #print (rrpartition)

                cursor.execute(table_range_query.format(r, ID_rating[0], ID_rating[1], ID_rating[2]))
                matching_range_tuple = cursor.fetchone()
                cursor.execute(table_rr_query.format(s, ID_rating[0], ID_rating[1], ID_rating[2]))
                matching_rr_tuple = cursor.fetchone()

                partition_query = 'SELECT * FROM {0} WHERE userid = {1} AND movieid = {2} AND rating = {3}'
                cursor.execute(partition_query.format(rr_partition_name, ID_rating[0], ID_rating[1], ID_rating[2]))
                #print (matching_partition)
                if (cursor.rowcount != 0):
                    #partition_name = ('rangeratingspart' + str(r),)
                    #rr_partition_name = ('roundrobinratingspart' + str(s),)
                    rows += (matching_range_tuple,)
                    rows += (matching_rr_tuple,)

    writeToFile("RangeQueryOut.txt", rows)


    pass



def PointQuery(ratingsTableName, ratingValue, openconnection):
    cursor = openconnection.cursor()

    #cursor.execute('''SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'range_part{0}' '''.format('%'))
    cursor.execute('''SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rangeratingspart{0}' '''.format('%'))
    range_partitions = cursor.fetchall()
    #cursor.execute('''SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rrobin_part{0}' '''.format('%'))
    cursor.execute('''SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'roundrobinratingspart{0}' '''.format('%'))
    rrobin_partitions = cursor.fetchall()
    rows = ()

    for r, partition in enumerate(range_partitions):
        #print (range_partitions)
        #print(partition)

        partition_name = ''.join(partition)
        #print(partition_name)
        rating_query = 'SELECT * FROM {0} WHERE rating = {1}'
        cursor.execute(rating_query.format(partition_name, ratingValue))
        ID_ratings = cursor.fetchall()


        for ID_rating in ID_ratings:

            for s, rr_partition in enumerate(rrobin_partitions):
                table_range_query = "SELECT 'rangeratingspart{0}', {1}, {2}, {3}"
                table_rr_query = "SELECT 'roundrobinratingspart{0}', {1}, {2}, {3}"
                rr_partition_name = ''.join(rr_partition)
                #print (rrpartition)

                cursor.execute(table_range_query.format(r, ID_rating[0], ID_rating[1], ID_rating[2]))
                matching_range_tuple = cursor.fetchone()
                cursor.execute(table_rr_query.format(s, ID_rating[0], ID_rating[1], ID_rating[2]))
                matching_rr_tuple = cursor.fetchone()

                partition_query = 'SELECT * FROM {0} WHERE userid = {1} AND movieid = {2} AND rating = {3}'
                cursor.execute(partition_query.format(rr_partition_name, ID_rating[0], ID_rating[1], ID_rating[2]))
                #print (matching_partition)
                if (cursor.rowcount != 0):
                    rows += (matching_range_tuple,)
                    rows += (matching_rr_tuple,)

    writeToFile("PointQueryOut.txt", rows)

    pass


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
