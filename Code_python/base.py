import os
import sys
from importlib import import_module
from contextlib import suppress
from urllib.request import urlopen
from subprocess import check_call
from datetime import datetime
from socket import gethostname
from glob import glob

def install_and_import(package_name, import_packg = None, version_pckg = ''):
    '''
    This function imports the package if possible, 
    if not it will check the internet connection to downlaod, 
    install and import the package

    # Parameters #
    --------------
    package_name: the name of the package to be used in the installation
    import_packg: the name of the package used in the 'import' function 
                  (it can be different from the package_name, e.g: 'console-menu')
    version_pckg: the version of the pachage you want to install
                  (example of initialization: version_pckg = '==1.19.2')
    '''
    if import_packg == None:
        import_packg = package_name

    try:
        # import the package
        print('Importing the package \'{}\': '.format(import_packg), end='')
        package = import_module(import_packg)
        print('"Successful"')

    except ImportError:
        # initialize the name of the package with its version
        install_package = package_name + version_pckg

        try:
            # Check internet connection
            urlopen('https://www.google.com')
            # Downbload and install the package
            print('\n')
            check_call([sys.executable, '-m', 'pip', 'install', install_package], shell=True)
            print('\n')

        except:
            # Error message when internet connection fails 
            print('\n\n> Unable to establish internet connection to download the package \'{0}\'.'.format(package_name))
            # Exit the program
            os._exit(1)

    finally:
        # Import the package
        package = import_module(import_packg)

    return package

# Import and install the needed packages
np = install_and_import('numpy', version_pckg = '==1.19.2')
pd = install_and_import('pandas', version_pckg = '==1.1.5')
pyodbc = install_and_import('pyodbc')
tabulate = install_and_import('tabulate')
consolemenu = install_and_import('console-menu', 'consolemenu')

from consolemenu import *
from consolemenu.items import *

class Table(pd.DataFrame):
    '''
    This class inherits from pandas.DataFrame.
    The instences of this class can be created from a dataframe, an SQL query or a CSV file
    '''
    # Add the attribute 'title' to the class which will be defined for each instance.
    _metadata = ['title']
    
    # Constructor
    @property
    def _constructor(self):
        return Table
    
    def __init__(self, *args, **kwargs):
        if args != ():
            super(Table, self).__init__(*args, **kwargs)

        elif kwargs != {}:
            # Extract the first key of the dictionary 'kwargs'
            key0 = next(iter(kwargs.keys()))
            # Construct a dataframe, inheriting from pd.DataFrame, from an SQL query.
            if key0 == 'query':
                try:
                    super(Table, self).__init__(pd.read_sql_query(kwargs[key0],
                                                                 kwargs['connec'], 
                                                                 params = kwargs['param']))
                    
                except Exception as exc:
                    print('Some occured error to execute the query:\n {}'.format(exc))
                    os._exit(1)

            # Construct a dataframe, inheriting from pd.DataFrame, from a CSV file.
            elif key0 == 'file_path':
                super(Table, self).__init__(pd.read_csv(kwargs[key0], sep=';'))

            # initialization of the title of the table
            self.title = kwargs['Title']

    def __str__(self):
        print('\n\t{}: \n\t'.format(self.title) + '=' * (len(self.title) + 1) + '\n')
        return (tabulate.tabulate(self, headers = 'keys', showindex = False, numalign = 'center', tablefmt = 'psql'))
   
    def compare_df(self, other):
        ''' 
        This function compares between two dataframes
        It sorts them by rows and columns and check if they are equal.
        It returns a boolean variable:
        - is_different: indicating if they are different or not
        '''
        # Sort the first dataframe by row and column and reset the index
        self = self.sort_values(by=self.columns.tolist())
        self = self.sort_index(axis = 1)
        self = self.reset_index(drop=True)

        # Sort the second dataframe by row and column and reset the index
        other = other.sort_values(by=other.columns.tolist())
        other = other.sort_index(axis = 1)
        other = other.reset_index(drop=True)

        # The equality test
        is_different = not(self.equals(other))

        return is_different

    def save_to_csv(self, name_csv, path):
        ''' 
        This function stores a dataframe in a CSV file in the current directory.
        It returns a boolean variable to confirm the success of the operation.
        - result = 'True':   if the file has been saved'.
        - result = 'False':  if the file has not been correctly saved.
        '''
        path_csv = os.path.join(path, name_csv)
        try:
            # Save the dataframe as CSV file in the hard drive
            self.to_csv(path_csv, sep=';', index=False)
            print('\n> The CSV file \'{}\' has been correctly saved in the hard drive.\n'.format(name_csv))
            result = True

        except:
            # The CSV file cannot be saved
            print('\n> Some error occured to save \'{}\' file\n'.format(name_csv))
            result = False

        return result
