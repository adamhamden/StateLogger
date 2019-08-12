# README

Creates and updates a state logging database.

This program allows you to log information to multiple levels in a database file, extract subsets based on certain condition, as well as converting such selections in pandas dataframes.

## Example 

1. Create the state logger object and give it a unique id and a database to write to (if left blank, one will automatically be created)

             logger = StateLogger(4,'robot-4_database1.db3')
2. Add messages of different type to the log
            
            logger.info(2346.4655855)
            logger.fatal('this is a fatal message')
3. Generate a list of rows that match a custom condition

            list_of_matches = logger.get_custom_condition("1==1")
    
4. Generate a pandas dataframe out of the list of matches
    
            df = logger.generate_pandas_data_frame_from_list(list_of_matches)
    
5. Display the list of matches

            logger.print_list_of_matches(list_of_matches)
            
## Run instructions
> Note, only tested with Python 3

0. Clone the repository

       git clone git@github.com:adamhamden/state-logger.git	
1. Go in the cloned directory

		cd state-logger

1. Start a virtual environment

		virtualenv -p python3 venv
		source venv/bin/activate

1. Install the requirements

		pip install -r requirements.txt
		
1. Check that the tests run

		python -m unittest state_logger_test.py
		
1. Run it!

		./state_logger/state_logger.py



