#!/usr/bin/env python3

import sqlite3
import pandas as pd
from prettytable import PrettyTable
import yaml

class Query:

    def __init__(self, list_of_matches, list_of_mismatched_types, header):
        self.header = header
        self.data = list_of_matches
        self.mismatched_data = list_of_mismatched_types

    def __repr__(self):

        x = PrettyTable()
        x.field_names = self.header

        for match in self.data:
            x.add_row(list(match))

        y = PrettyTable()
        y.field_names = self.header

        for match in self.mismatched_data:
            y.add_row(list(match))

        return str(x) + '\nMismatched data types below\n' + str(y)

    def get(self):
        return pd.DataFrame(self.data)

    def get_mistmatched_types(self):
        return pd.DataFrame(self.mismatched_data)

class StateLogger:

    def __init__(self):

        with open("../config.yml", 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        self.robot_id = int(cfg["sql_database"]["robot_id"])
        self.sql_database =  cfg["sql_database"]["database_name"]
        self.keepLocalCopy = cfg["sql_database"]["keep_local_copy"]

        self.database = sqlite3.connect(self.sql_database)
        self.cursor = self.database.cursor()

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "log(" +
                            "timestamp TEXT NOT NULL, " +
                            "robot_id TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL" +
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "mismatched_type_log(" +
                            "timestamp TEXT NOT NULL, " +
                            "robot_id TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL" +
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_log(" +
                            "timestamp TEXT NOT NULL, " +
                            "robot_id TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL" +
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_mismatched_type_log(" +
                            "timestamp TEXT NOT NULL, " +
                            "robot_id TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL" +
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "topics(" +
                            "topic_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "topic_name TEXT NOT NULL, " +
                            "data_type TEXT NOT NULL" +
                            ")"
                            )

    def __del__(self):
        if self.keepLocalCopy:
            self.local_db_name = "local_" + self.sql_database
            self.local_db_connection = sqlite3.connect(self.local_db_name)
            self.cursor.execute("ATTACH DATABASE ? as local_db", (self.local_db_name,))
            self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                                "local_db.local_log(" +
                                "timestamp TEXT NOT NULL, " +
                                "robot_id TEXT NOT NULL, " +
                                "topic_id TEXT NOT NULL, " +
                                "data BLOB NOT NULL" +
                                ")"
                                )
            self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                                "local_db.local_mismatched_type_log(" +
                                "timestamp TEXT NOT NULL, " +
                                "robot_id TEXT NOT NULL, " +
                                "topic_id TEXT NOT NULL, " +
                                "data BLOB NOT NULL" +
                                ")"
                                )
            self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                                "local_db.topics(" +
                                "topic_id INTEGER, " +
                                "topic_name TEXT NOT NULL, " +
                                "data_type TEXT NOT NULL" +
                                ")"
                                )
            self.cursor.execute("INSERT INTO local_db.local_log SELECT * FROM local_log;")
            self.cursor.execute("INSERT INTO local_db.local_mismatched_type_log SELECT * FROM local_mismatched_type_log;")
            self.cursor.execute("INSERT INTO local_db.topics SELECT * FROM topics;")

        self.cursor.execute("DROP TABLE IF EXISTS local_log;")
        self.cursor.execute("DROP TABLE IF EXISTS local_mismatched_type_log;")
        self.local_db_connection.commit()
        self.database.commit()


    def write(self, topic_name, data, is_keep_local_copy=False):

        self.cursor.execute("SELECT * from topics WHERE topic_name == ?", (topic_name,))
        topic_matches = self.cursor.fetchall()
        if len(topic_matches) != 1:
            raise ValueError("Attempting to write to an invalid topic name")

        topic_id = topic_matches[0][0]
        topic_data_type = topic_matches[0][2]
        insert_data_type = str(type(data).__name__)

        values_to_insert = (self.robot_id, topic_id, data)

        if topic_data_type == insert_data_type:
            self.cursor.execute("INSERT INTO log VALUES (DATETIME('now'),?,?,?)", values_to_insert)
        else:
            self.cursor.execute("INSERT INTO mismatched_type_log VALUES (DATETIME('now'),?,?,?)", values_to_insert)

        if is_keep_local_copy and topic_data_type == insert_data_type:
            self.cursor.execute("INSERT INTO local_log VALUES (DATETIME('now'),?,?,?)", values_to_insert)
        elif is_keep_local_copy:
            self.cursor.execute("INSERT INTO local_mismatched_type_log VALUES (DATETIME('now'),?,?,?)", values_to_insert)

        self.database.commit()

    def add_topic(self, topic_name, data_type):

        self.cursor.execute("SELECT * from topics WHERE topic_name == ?", (topic_name,))
        topic_matches = self.cursor.fetchall()
        if len(topic_matches) != 0:
            raise ValueError("Attempting add an existing topic")

        self.cursor.execute("INSERT INTO topics VALUES (NULL, ?, ?)", (topic_name, data_type,))

    def get_query(self, condition):

        execute_statement = "SELECT * FROM log WHERE " + condition
        self.cursor.execute(execute_statement)
        list_of_matches = self.cursor.fetchall()

        execute_statement = "SELECT * FROM mismatched_type_log WHERE " + condition
        self.cursor.execute(execute_statement)
        list_of_mismatched_types = self.cursor.fetchall()
        if len(list_of_mismatched_types) != 0:
            print("Warning, there are mismatched data types that satisfy this query!")

        self.cursor.execute("PRAGMA table_info(log)")
        pragma_list = self.cursor.fetchall()
        header_list = [i[1] for i in pragma_list]

        query = Query(list_of_matches, list_of_mismatched_types, header_list)

        return query

if __name__ == "__main__":

    logger = StateLogger()
    logger.add_topic('age', 'int')
    logger.add_topic('ages', 'int')
    logger.write('age', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 12345, True)
    logger.write('ages', 'ummmm', True)
    logger.write('ages', 12345, True)

    query = logger.get_query("1==1")
    print(query)