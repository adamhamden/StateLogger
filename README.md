# README

Creates and updates a state logging database.

This program allows you to log information to different topics stored in a database file, extract subsets based on certain condition, as well as converting such selections into pandas dataframes.

## Example 

1. Create a config.yml file in the home directory that mirrors the following

        sql_database:
          robot_id: 1
          database_name: some_database_name.db3
          keep_local_copy: True

1. Create the state logger object and give it a unique id and a database to write to (if left blank, one will automatically be created)

             logger = StateLogger()
1. Add a topic and its respective data type
             
             logger.add_topic('Age', 'int')
2. Add integer message with the Age topic and allow for local backup
            
            logger.write('Age', 2346, True)
3. Generate a list of rows that match a custom condition

            query = logger.get("1==1")
    
4. Generate a pandas dataframe out of the list of matches
    
            df = query.get()
    
5. Display the list of matches

            print(query)
            
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



