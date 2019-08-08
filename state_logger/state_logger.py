#!/usr/bin/env python3

import sqlite3
import time
import pandas as pd
from prettytable import PrettyTable

def create_database(robot_id):
    dt = int(1000000000*time.time())
    db_file = "./"+"robot-"+str(robot_id)+"_"+str(dt)+".db3"
    conn = sqlite3.connect(db_file)
    return conn


class StateLogger:
    def __init__(self, robot_id, sql_database=None):
        self.robot_id = robot_id
        self.sql_database = sql_database

        if self.sql_database is None:
            self.connection = create_database(self.robot_id)
        else:
            self.connection = sqlite3.connect(sql_database)

        self.cursor = self.connection.cursor()

    def __del__(self):
        self.connection.commit()


    def _write(self, level, data, is_keep_local_copy=False):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS log(timestamp INTEGER8 NOT NULL, robot_id INTEGER NOT NULL, log_level TEXT NOT NULL, data_type TEXT NOT NULL, data BLOB NOT NULL);")

        dt = int(1000000000*time.time())
        data_type = str(type(data).__name__)
        values_to_insert = (dt, self.robot_id, level, data_type, data)
        self.cursor.execute("INSERT INTO log VALUES (?,?,?,?,?)", values_to_insert)

        if is_keep_local_copy:
            self.cursor.execute("CREATE TABLE IF NOT EXISTS local_log(timestamp INTEGER8 NOT NULL, robot_id INTEGER NOT NULL, log_level TEXT NOT NULL, data_type TEXT NOT NULL, data BLOB NOT NULL);")
            self.cursor.execute("INSERT INTO local_log VALUES (?,?,?,?,?)", values_to_insert)

        self.connection.commit()

    def debug(self, data):

        self._write('DEBUG', data, False)

    def info(self, data):

        self._write('INFO', data, False)

    def warn(self, data):

        self._write('WARN', data, False)

    def error(self, data):

        self._write('ERROR', data, True)

    def fatal(self, data):

        self._write('FATAL', data, True)

    def get_in_bound_data(self, time1, time2):

        if time1 > time2:
            return

        self.cursor.execute("SELECT * FROM log WHERE timestamp > ? AND timestamp < ?", (str(time1), str(time2)))
        list_of_matches = self.cursor.fetchall()
        return list_of_matches

    def get_custom_condition(self, condition):

        execute_statement = "SELECT * FROM log WHERE " + condition
        self.cursor.execute(execute_statement)
        list_of_matches = self.cursor.fetchall()
        return list_of_matches

    def generate_pandas_data_frame_from_condition(self, condition):

        data_frame = pd.read_sql_query("SELECT * FROM log WHERE %s" % condition, self.connection)
        return data_frame

    def generate_pandas_data_frame_from_list(self, list_of_matches):

        data_frame = pd.DataFrame(list_of_matches)
        return data_frame

    def display_in_bound_data(self, time1, time2):

        list_of_matches = self.get_in_bound_data(time1, time2)
        self.print_list_of_matches(list_of_matches)

    def display_custom_condition(self, condition):

        list_of_matches = self.get_custom_condition(condition)
        self.print_list_of_matches(list_of_matches)

    def print_list_of_matches(self, list_of_matches):

        self.cursor.execute("PRAGMA table_info(log)")
        pragma_list = self.cursor.fetchall()
        header_list = [i[1] for i in pragma_list]

        x = PrettyTable()
        x.field_names = header_list

        for match in list_of_matches:
            x.add_row(list(match))

        print(x)


if __name__ == "__main__":

    logger = StateLogger(4,'robot-4_1565195429906824448.db3')
    logger.info(2346.4655855)
    logger.fatal('this is a fatal message')
    list = logger.get_custom_condition("1==1")
    df = logger.generate_pandas_data_frame_from_list(list)
    print(df)