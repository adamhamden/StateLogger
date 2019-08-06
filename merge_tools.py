#!/usr/bin/env python3

import sqlite3
import datetime
import sys
import os
import argparse
# we get two argument inputs
# we create a new db
# we create a new table in that db if it doesnt exist
# call union all on the new values
directory = None

def create_database():
    d = datetime.datetime.today()
    global directory
    directory = d.isoformat() + "_merged_ros2bag"
    os.mkdir(directory)
    db_file = "./"+directory+"/" + "ros2bag.db3"
    conn = sqlite3.connect(db_file)
    return conn

def find_max_message_id(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("select max(id) from messages")
    return cursor.fetchone()[0]

def increment_message_id(incr, db_conn):
    cursor = db_conn.cursor()
    incr = int(incr)
    cursor.execute("update messages set id = id + :incr", {"incr": incr})
    db_conn.commit()

def generate_merged_database(merge_db_conn):
    cursor = merge_db_conn.cursor()

    attachDatabaseSQL = "ATTACH DATABASE ? AS db1"
    dbName = (sys.argv[1],)
    cursor.execute(attachDatabaseSQL, dbName)

    attachDatabaseSQL = "ATTACH DATABASE ? AS db2"
    dbName = (sys.argv[2],)
    cursor.execute(attachDatabaseSQL, dbName)

    cursor.execute("CREATE TABLE topics(id INTEGER PRIMARY KEY,name TEXT NOT NULL,type TEXT NOT NULL,serialization_format TEXT NOT NULL);")
    cursor.execute("CREATE TABLE messages(id INTEGER PRIMARY KEY,topic_id INTEGER NOT NULL,timestamp INTEGER NOT NULL, data BLOB NOT NULL);")
    cursor.execute("CREATE INDEX timestamp_idx ON messages (timestamp ASC);")

    cursor.execute("INSERT INTO topics select * from db1.topics;")
    cursor.execute("INSERT INTO messages select id,topic_id,timestamp, data from db1.messages union all select id,topic_id,timestamp, data from db2.messages;")

    merge_db_conn.commit()

def generate_metadata(db_conn):
    global directory
    metadata_file = "./"+directory+"/" + "metadata.yaml"
    file = open(metadata_file, "w")
    file.write("rosbag2_bagfile_information:\n")
    file.write("  version: 1\n")
    file.write("  storage_identifier: sqlite3\n")
    file.write("  relative_file_paths:\n")
    file.write("    - ros2bag.db3\n")


    cursor = db_conn.cursor()
    cursor.execute("select min(timestamp) from db1.messages")
    min_db1 = cursor.fetchone()[0]
    cursor.execute("select max(timestamp) from db1.messages")
    max_db1 = cursor.fetchone()[0]

    db1_dur = max_db1 - min_db1

    cursor.execute("select min(timestamp) from db2.messages")
    min_db2 = cursor.fetchone()[0]
    cursor.execute("select max(timestamp) from db2.messages")
    max_db2 = cursor.fetchone()[0]

    db2_dur = max_db2 - min_db2

    total_dur = db1_dur + db2_dur

    file.write("  duration:\n")
    file.write("    nanoseconds: %d\n" % total_dur)
    file.write("  starting_time:\n")
    file.write("    nanoseconds_since_epoch: %d\n" % min_db1)

    cursor.execute("select max(id) from messages")
    total_messages = cursor.fetchone()[0]
    file.write("  message_count: %d\n" % total_messages)

    file.write("  topics_with_message_count:\n")
    file.write("    - topic_metadata:\n")

    cursor.execute("select name, type, serialization_format from topics")
    fetched = cursor.fetchone()
    topic_name = fetched[0]
    topic_type = fetched[1]
    topic_serial = fetched[2]

    file.write("        name: %s\n" % topic_name)
    file.write("        type: %s\n" % topic_type)
    file.write("        serialization_format: %s\n" % topic_serial)
    file.write("      message_count: %d" % total_messages)


def merge_databases(db1, db2):

    connection = create_database()
    db1_conn = sqlite3.connect(db1)
    db2_conn = sqlite3.connect(db2)
    incr = find_max_message_id(db1_conn)
    increment_message_id(incr, db2_conn)

    generate_merged_database(connection)
    generate_metadata(connection)

    db1_conn.commit()
    db2_conn.commit()
    connection.commit()
    db1_conn.close()
    db2_conn.close()
    connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Manipulate databases.')
    parser.add_argument('-m', dest='merge', nargs=2)
    args = parser.parse_args()
    print(args.merge)
    #merge_databases(sys.argv[1], sys.argv[2])


