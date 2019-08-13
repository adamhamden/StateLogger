import state_logger.state_logger as sl
import unittest
import time
import os

class TestStateLogger(unittest.TestCase):

    def __del__(self):
        os.system('rm *.db3')

    def test_add_new_topic(self):
        logger = sl.StateLogger()

        try:
            logger.add_topic('Age', 'int')
            logger.add_topic('Name', 'string')
            logger.add_topic('Balance', 'float')
        except:
            self.fail()

    def test_add_duplicate_topic(self):
        logger = sl.StateLogger()

        logger.add_topic('topic', 'int')
        with self.assertRaises(ValueError):
            logger.add_topic('topic', 'int')

    def test_write_to_valid_topic(self):

        logger = sl.StateLogger()
        logger.add_topic('topic_int', 'int')
        logger.add_topic('topic_string', 'str')

        logger.write('topic_int', 1234)
        logger.write('topic_string', "Hello, world!")

        test_query = logger.get_query("1==1")
        data = test_query.get()

        self.assertEqual(data.loc[0, 3], 1234)
        self.assertEqual(data.loc[1, 3],  "Hello, world!")


    def test_write_incorrect_type(self):

        logger = sl.StateLogger()
        logger.add_topic('topic_int', 'int')
        logger.add_topic('topic_string', 'string')

        logger.write('topic_int', "Hello, world!")
        logger.write('topic_string', "Hello, world!")

        test_query = logger.get_query("1==1")
        data = test_query.get_mistmatched_types()

        self.assertEqual(data.loc[0, 3],  "Hello, world!")