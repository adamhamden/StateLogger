import state_logger.state_logger as sl
import unittest
import time


class TestStateLogger(unittest.TestCase):

    def test_write_function(self):
        logger = sl.StateLogger(1)

        logger.debug("This is an debug message")
        logger.info("This is an info message")
        logger.warn("This is a warning message")
        logger.error("This is an error message")
        logger.fatal('This is a fatal message')

        all_rows_list = logger.get_query("1==1")

        self.assertEqual(all_rows_list[0][4], "This is an debug message")
        self.assertEqual(all_rows_list[1][4], "This is an info message")
        self.assertEqual(all_rows_list[2][4], "This is a warning message")
        self.assertEqual(all_rows_list[3][4], "This is an error message")
        self.assertEqual(all_rows_list[4][4], "This is a fatal message")

    def test_get_in_bound_data_nominal(self):
        logger = sl.StateLogger(2)

        start_time = int(1000000000*time.time())

        logger.debug(1)
        logger.info(2)
        logger.warn(3)
        logger.error(4)
        logger.fatal(5)

        end_time = int(1000000000*time.time())

        list_of_in_bound_data = logger.get_in_bound_data(start_time, end_time)

        self.assertEqual(list_of_in_bound_data[0][4], 1)
        self.assertEqual(list_of_in_bound_data[1][4], 2)
        self.assertEqual(list_of_in_bound_data[2][4], 3)
        self.assertEqual(list_of_in_bound_data[3][4], 4)
        self.assertEqual(list_of_in_bound_data[4][4], 5)

    def test_get_in_bound_data_boundary(self):
        logger = sl.StateLogger(2)

        start_time = int(1000000000 * time.time())

        logger.debug(1)

        end_time = int(1000000000 * time.time())

        logger.info(2)
        logger.warn(3)
        logger.error(4)
        logger.fatal(5)

        list_of_in_bound_data = logger.get_in_bound_data(start_time, end_time)

        self.assertEqual(list_of_in_bound_data[0][4], 1)

    def test_get_in_bound_data_off_nominal(self):
        logger = sl.StateLogger(2)

        start_time = int(1000000000*time.time())

        logger.debug(1)
        logger.info(2)
        logger.warn(3)
        logger.error(4)
        logger.fatal(5)

        end_time = int(1000000000*time.time())

        list_of_in_bound_data = logger.get_in_bound_data(end_time, start_time)

        self.assertEqual(list_of_in_bound_data, [])

    def test_get_custom_condition(self):
        logger = sl.StateLogger(2)

        logger.debug(1)
        logger.info(2)
        logger.warn(3)
        logger.error(4)
        logger.fatal(5)

        list_of_matches = logger.get_query("1==1")

        self.assertEqual(list_of_matches[0][4], 1)
        self.assertEqual(list_of_matches[1][4], 2)
        self.assertEqual(list_of_matches[2][4], 3)
        self.assertEqual(list_of_matches[3][4], 4)
        self.assertEqual(list_of_matches[4][4], 5)

        list_of_matches = logger.get_query("1==0")

        self.assertEqual(list_of_matches, [])

    def test_generate_pandas_dataframe(self):
        logger = sl.StateLogger(2)

        logger.debug(1)
        logger.info(2)
        logger.warn(3)
        logger.error(4)
        logger.fatal(5)


        df = logger.get_pandas_data_frame_from_query("1==1")

        self.assertEqual(df["data"].mean(), 3)