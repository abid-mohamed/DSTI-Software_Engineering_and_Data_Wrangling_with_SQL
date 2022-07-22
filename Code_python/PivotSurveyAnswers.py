from CompTools import *

def main():
    ## Initialization of parameters ##
    #--------------------------------#
    #   - name of the database (it can be changed in the menu if it doesn't 
    #   correspond to an existing database)
    my_database = 'Survey_Sample_A19'

    #   - name of the view in the database
    view_table_db_name = 'vw_AllSurveyData'

    #   - directory of the data
    data_dir = 'Data_PivotSurveyAnswers'
    my_path = create_dir(data_dir)
    my_norm_path = os.path.normpath(my_path)

    #   - Extract the current time and date to be used for the names 
    #   of the 'SurveyStructure' and 'AllSurveyData' files.
    current_datetime = datetime.now()
    current_datetime = current_datetime.strftime('%Y_%m_%d-%H%M%S')

    #   - Initialize the name of the files in the hard drive
    surveyStructure_name = 'SurveyStructure-{}.csv'.format(current_datetime)
    allSurveyData_name = 'vw_AllSurveyData-{}.csv'.format(current_datetime)

    ## Create a connection to the database ##
    #---------------------------------------#

    # Automatically create a connection to the database
    odbc_connection = connection_to_db(my_database)

    # Manually create a connection to the database
    if odbc_connection is None:
        print('Impossible to establish an automatic connection to the database "{}".\n'.format(my_database))
        
        # Create the root menu
        menu = ConsoleMenu("DataBase connection menu", "These are the possible choices:", 
                           epilogue_text=('Please select your entry'),
                           exit_option_text='Exit Application')
        
        # Choice #1: Each parameter will be entered one by one by the user
        function_item_1 = FunctionItem('Enter manually the parameters of the connection',
                                       connec_menu_option, args=[1], should_exit=True)
        # Choice #2: The total connection will be entered in the same text by the user 
        function_item_2 = FunctionItem('Enter all the connection parameters in the same text',
                                       connec_menu_option, args=[2], should_exit=True)

        # Add all the items to the root menu
        menu.append_item(function_item_1)
        menu.append_item(function_item_2)

        # Show the menu
        menu.start()
        menu.join()

        # Extract the result of the selected choice.
        result1 = function_item_1.get_return()
        result2 = function_item_2.get_return()

        if result1 is not None:
            odbc_connection = result1
        else:
            odbc_connection = result2

    if odbc_connection is None:
        print('> Impossible to establish database connection.\n')
    else:
        ## Extract the table 'dbo.SurveyStructure' from the database ##
        #-------------------------------------------------------------#

        # Parameters fed to the class 'Table' to construct a dataframe 
        #  by applying an SQL query to the table 'SurveyStructure'.
        survStruct_param = {'query': 'SELECT * FROM [dbo].[SurveyStructure]',
                            'connec': odbc_connection,
                            'param': None,
                            'Title': 'The table "SurveyStructure" in the database'}

        # Create the instance 'df_survStruct' of the class 'Table' 
        #  from the query contained in 'survStruct_param'
        df_survStruct = Table(**survStruct_param)
        print(df_survStruct)

        ## Compare between 'SurveyStructure' from the database and ##
        #  the CSV file in the hard drive and update the CSV file   #
        #-----------------------------------------------------------#
        modification_test =  compare_update_file(df_survStruct, surveyStructure_name, my_path)

        if modification_test is True:
            ## Create and execute the query to create or alter the view ##
            #          'vw_AllSurveyData' in the database                #
            #------------------------------------------------------------#
            refresh_survey_view(odbc_connection, view_table_db_name)

            # Parameters fed to the class 'Table' to construct a dataframe 
            #  by applying an SQL query to the table 'vw_AllSurveyData'.
            view_table_param = {'query': 'SELECT * FROM [{}]'.format(view_table_db_name),
                                'connec': odbc_connection,
                                'param': None,
                                'Title': 'The view "vw_AllSurveyData" in the database (a sample of 10 rows)'}

            # Create the instance 'df_view_table' of the class 'Table' 
            #  from the query contained in 'view_table_param'
            df_view_table = Table(**view_table_param)
            # Replace the 'None' value by 'nan' 
            df_view_table = df_view_table.fillna(value=np.nan)

            # Extract a samples of 10 rows from the dataframe 'df_view_table'.
            df_view_table_sample = df_view_table.sample(10)
            # Sort the sample 'df_view_table_sample' by the first column.
            df_view_table_sample = df_view_table_sample.sort_values(by='UserId')
            # print the sample 'df_view_table_sample' as type string to avoid scientific notation.
            print(df_view_table_sample.astype(str))

            # Extract a pivoted survey data in a CSV file
            df_view_table.save_to_csv(allSurveyData_name, my_path)
            print('The CSV files are located in :\n  {}\n'.format(my_norm_path))
        # Close the connection
        odbc_connection.close()

    print('Thank you.\n')
     
if __name__ == '__main__':
    main()