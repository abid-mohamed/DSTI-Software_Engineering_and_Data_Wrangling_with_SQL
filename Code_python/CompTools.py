from base import *

def create_dir(my_dir):
    '''
    This function creates a folder 'my_dir' to store data if does not exist.
    If the folder has not been created, the data is stored in the current work directory.
    This function returns the path of the new folder created 'my_dir' 
     or the path of the current work directory.
    '''
    # Get the current work directory
    dir_name = os.getcwd()

    # Create a directory, if it does not exist, to store the data
    with suppress(Exception):
        os.mkdir(my_dir)

    # Create the total directory path to save data
    path = os.path.join(dir_name, my_dir)

    # Verify if the folder is correctly created
    if os.path.isdir(path):
        print('\nThe folder in which data are saved is \'{}\''.format(my_dir))
        return path
    else:
        return dir_name

def connection_to_db(database_name, connec_str = None, driver_name = None, server_name = None, uid_name = None, password = None):
    ''' 
    This function creates a connection to the database 'database_name'.
    If the 'connec_str' is not 'None' it uses it to create the connection.
    Otherwise, it uses the parameters 'driver_name' and 'server_name' and 'uid_name' if they are not 'None'
    In case they are 'None', it automatically searches their correspending values and set 'Trusted_Connection=yes;'.
    This function returns a connection object.
    '''
    # Initialize the connection 'sql_connection' as 'None'
    sql_connection = None

    if database_name != '':
        if connec_str is None:
            print('\nConnecting to the database \'{}\' with the following parameters:\n'.format(database_name))

            if (driver_name is None) or (driver_name == ''):
                # Get all the relevent drivers of the system
                drivers_list = pyodbc.drivers()
                # Select the newest version of 'ODBC Driver' 
                drivers_odbc = [driver for driver in drivers_list if driver.upper().startswith('ODBC DRIVER')]
                driver_name = sorted(drivers_odbc, reverse=True)[0]
            print('\tThe name of the driver: {}'.format(driver_name))

            if (server_name is None) or (server_name == ''):
                # Get the name of the machine
                server_name = gethostname()
            print('\tThe name of the server: {}'.format(server_name))

            # Update the connection string
            connection_string = 'DRIVER={{{0}}};SERVER={1};DATABASE={2};'.format(driver_name, server_name, database_name)
    
            
            if (uid_name is None) or (uid_name == ''):
                # Set 'Trusted_Connection' to yes if 'uid_name' is None or ''
                connection_string = ''.join([connection_string, 'Trusted_Connection=yes;'])
                print('\tAuthentification mode : "Windows authentification"\n')

            else:
                # Update the connection string with 'uid_name' and 'password'
                uid_password = 'UID={0};PWD={1};'.format(uid_name, password)
                connection_string = ''.join([connection_string, uid_password])
                print('\tAuthentification mode :  User ID : {0}\n\t\t\t\t Password: {1}\n'.format(uid_name, password))

        else:
            print('\nConnecting to the database with the following parameters:\n\t{}\n'.format(connec_str))
            connection_string = connec_str

    try:
        # create a connection to the database
        sql_connection = pyodbc.connect(connection_string)
        print('Successfully connected.\n')

    except pyodbc.Error as err:
        print('\nConnection error to the database. Error Code: {}\n'.format(err.args[0]))
        print('Message: \n{}\n'.format(err.args[1]))

    except Exception as exc:
        print('\nA connection error occured: \n{}\n'.format(exc))

    return sql_connection

def connec_menu_option(conn_type):
    '''
    This function is run when the automatic connection failed.
    it allows the user to enter manually the parameters to connect to the database.
    Two types of manual connection are supported.

    Connection type 1:
    The user must enter the name of the database.
    The other parameters: name of the driver and  name of the server, if not entered manually will be automatically looked for by the program.
    If the user didn't enter the "user name (uid_nm)" the connection is created with "Trusted_Connection=yes"

    Connection type 2:
    The user should enter manually all the parameters in the same text, e.g:
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-GHJ1950;DATABASE=Survey_Sample_A19;Trusted_Connection=yes"
    '''
    if conn_type == 1:
        conn_str = None
        db_nm = input('\n\tEnter the name of the database: ')       # e.g.: 'Survey_Sample_A19'
        driver_nm = input('\n\tEnter the name of the driver  : ')   # e.g.: 'ODBC Driver 17 for SQL Server'
        server_nm = input('\n\tEnter the name of the server  : ')   # e.g.: 'DESKTOP-GHJ1950'
        uid_nm = input('\n\tEnter the the user ID         : ')      # e.g.: 'sa'

        if uid_nm != '':
            passwd = input('\n\tEnter the password            : ')  # e.g.: '****'
        else:
            passwd = None

    else:
        conn_str = input('''\n\tEnter the text of the connection:
        e.g.: DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-GHJ1950;DATABASE=Survey_Sample_A19;Trusted_Connection=yes \n\n\t>> ''')
        db_nm, driver_nm, server_nm, uid_nm, passwd = [None] * 5
        
    # Create a connection to the database
    sql_connec = connection_to_db(database_name = db_nm,
                                  connec_str = conn_str,
                                  driver_name = driver_nm,
                                  server_name = server_nm,
                                  uid_name = uid_nm,
                                  password = passwd)

    return sql_connec 

def view_menu_option(df_file_db, file_name_hd, path):
    '''
    This function is run when the user chooses Yes in the 'View menu'
     to create a new "csv " file.
    For that it calls the method 'save_to_csv' of 'Table' object and returns a boolean value.
    '''
    return df_file_db.save_to_csv(file_name_hd, path) 

def delete_old_files(list_of_files):
    '''
    This function deletes all the elements from the list in the hard drive 
     provided that it does not encounter any issue.
    '''
    if len(list_of_files) > 0:
        for file in list_of_files:
            with suppress(Exception):
                os.remove(file)

def compare_update_file(df_file_db, file_name_hd, path):
    '''
    This function compares the csv file with the dataframe and possibly update it: 
     it saves its newest version and deletes all older versions of it.
    It returns a boolean variable with values:
    - is_different = True: if the new update is saved
    - is_different = False: if there is no need to modify the csv file.
    '''
    # The new name of the file in the hard drive
    file_name = file_name_hd.split('-')[0]

    # List of all CSV files sorted by date descending order
    path_csv = os.path.join(path, '{}-*_*_*-*.csv'.format(file_name))
    list_csv_file = glob(path_csv)
    list_csv_file = sorted(list_csv_file, reverse=True)

    if list_csv_file == []:
        # In this case no CSV file exists in the directory
        # A copy of the dataframe is stored in the CSV file
        print('The CSV file {} does not exist in the had drive \n\nCreation of a new CSV file.'.format(file_name))
        is_different = df_file_db.save_to_csv(file_name_hd, path)

    else:
        try:
            # Create the instance 'df_file_hd' of the class 'Table' from a CSV file
            df_file_hd = Table(**{'file_path': list_csv_file[0],
                                  'Title': 'The table "SurveyStructure" in the hard drive'})

            # Compare between the file in the hard drive and the dataframe (file in the database)
            is_different = df_file_db.compare_df(df_file_hd)
            print(df_file_hd)

            if is_different is True:
                # In this case: the CSV file is different from the dataframe
                print('> The two tables of "{}" in the database and in the hard drive are NOT the same.\n'.format(file_name))
                # Store the dataframe in a new CSV file
                is_different = df_file_db.save_to_csv(file_name_hd, path)

            else:
                # In this case: the CSV file is equal to the dataframe
                print('\n> The two tables of "{}" in the database and in the hard drive are the same.\n'.format(file_name))

        except IOError:
            # In this case we cannot have access to the CSV file in the hard drive. No comparison with the dataframe will be performed.
            print('{} file is not accessible \n\nCreation of a new file.'.format(file_name))

            # A copy of the dataframe is stored in the CSV file
            is_different = save_df_to_csv(df_file_db, file_name_hd)

    # Delete of all older versions of the CSV file in the hard drive if they exist.
    if is_different is True:
        delete_old_files(list_csv_file)
    else:
        delete_old_files(list_csv_file[1:])
        print('> The view will not be generated.\n')

    return is_different

def get_all_suvey_data(sql_connec):
    '''
    This function replicates the algorithm of the dbo.fn_GetAllSurveyDataSQL stored function
    It constructs the query that will be applied to the database to create the view.
    '''
    strQueryTemplateForAnswerColumn = '''
        COALESCE(
        (
	        SELECT a.Answer_Value
	        FROM Answer as a
	        WHERE
		        a.UserId = u.UserId
		        AND a.SurveyId = <SURVEY_ID>
		        AND a.QuestionId = <QUESTION_ID>
        ), -1) AS ANS_Q<QUESTION_ID> '''


    strQueryTemplateForNullColumnn = '''
        NULL AS ANS_Q<QUESTION_ID> '''

    strQueryTemplateOuterUnionQuery = '''
    SELECT
	    UserId, 
        <SURVEY_ID> as SurveyId, 
        <DYNAMIC_QUESTION_ANSWERS>
    FROM
	    [User] as u
    WHERE EXISTS
    (
		SELECT *
		FROM Answer as a
		WHERE u.UserId = a.UserId
		AND a.SurveyId = <SURVEY_ID>
    )'''

    strFinalQuery = ''

    survey_param = {'query': 'SELECT SurveyId FROM [dbo].[Survey] ORDER BY SurveyId',
                    'connec': sql_connec,
                    'param': None,
                    'Title': 'The column "SurveyId" of the table "Survey" in the database'}

    df_SurveyID = Table(**survey_param)

    # main loop, over all the surveys.
    for ind_SurveyID, currentSurveyId in enumerate(df_SurveyID.SurveyId):
        # Constrauct the answer column queries for each survey 
        question_query = '''SELECT *
					        FROM
					        (
						        SELECT
							        SurveyId,
							        QuestionId,
							        1 as InSurvey
						        FROM
							        SurveyStructure
						        WHERE
							        SurveyId = ?
						        UNION
						        SELECT 
							        ? as SurveyId,
							        Q.QuestionId,
							        0 as InSurvey
						        FROM
							        Question as Q
						        WHERE NOT EXISTS
						        (
							        SELECT *
							        FROM SurveyStructure as S
							        WHERE S.SurveyId = ? 
                                      AND S.QuestionId = Q.QuestionId
						        )
					        ) as t
					        ORDER BY QuestionId;'''

        param_quest_query = [currentSurveyId] * 3

        # Parameters fed to the class 'Table' to construct a dataframe from an SQL query.
        # The parameters in 'param_quest_query' are passed to 'question_query' 
        #  in a way that avoids SQL-injection.
        currentQuestion_param = {'query': question_query,
                                 'connec': sql_connec,
                                 'param': param_quest_query,
                                 'Title': 'The table current question'}
        # Create the instance 'df_currentQuestion' of the class 'Table' 
        # from the query contained in 'currentQuestion_param'
        df_currentQuestion = Table(**currentQuestion_param)

        strColumnsQueryPart = ''

        # Inner loop iterates over the question.
        for ind_QuestionID, currentQuestionID in enumerate(df_currentQuestion.QuestionId):

            currentInSurvey = df_currentQuestion['InSurvey'][ind_QuestionID]
            
            if currentInSurvey == 0:
                # In this case question is not in the current survey.
                # The value of this column will be NULL
                strColumnsQueryPart = ''.join([strColumnsQueryPart, 
                                               strQueryTemplateForNullColumnn.replace('<QUESTION_ID>', str(currentQuestionID))])

            else:
                # In this case question is in the current survey.
                strColumnsQueryPart = ''.join([strColumnsQueryPart,
                                               strQueryTemplateForAnswerColumn.replace('<QUESTION_ID>', str(currentQuestionID))])
            
            if (ind_QuestionID + 1) != len(df_currentQuestion):
                # Place a comma between column statements, except for the last one.
                strColumnsQueryPart = ''.join([strColumnsQueryPart, ' , '])

        strCurrentUnionQueryBlock = strQueryTemplateOuterUnionQuery.replace('<DYNAMIC_QUESTION_ANSWERS>', strColumnsQueryPart)

        strCurrentUnionQueryBlock = strCurrentUnionQueryBlock.replace('<SURVEY_ID>', str(currentSurveyId))

        strFinalQuery = ''.join([strFinalQuery, strCurrentUnionQueryBlock])

        if (ind_SurveyID + 1) != len(df_SurveyID.SurveyId):
            strFinalQuery = ''.join([strFinalQuery, ''' UNION '''])

    return(strFinalQuery)  

def refresh_survey_view(sql_connec, view_table_db):
    '''
    This function run the SQL query on the database to create the view.
    '''
    # Create the SQl query text
    str_query = get_all_suvey_data(sql_connec)
    survey_data_query = 'CREATE OR ALTER VIEW {} AS ( {} );'.format(view_table_db, str_query)
    # Create a cursor from the SQL connection
    curs = sql_connec.cursor()

    try:
        # Run the created SQL query on the database
        curs.execute(survey_data_query)
        print('> The view {} has been successfully created or updated in the database.'.format(view_table_db))
    except Exception as e:
        print(e)

    # Close the cursor
    curs.close()
    return