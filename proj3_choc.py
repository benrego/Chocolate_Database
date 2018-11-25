import sqlite3
import sys
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db.sqlite'
db_name = DBNAME
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def init_db():
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()


    # Drop Tables
    statement = "DROP TABLE IF EXISTS 'Bars';"
    cur.execute(statement)

    statement = "DROP TABLE IF EXISTS 'Countries';"
    cur.execute(statement)

    conn.commit()

    statement = '''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT,
            'SpecificBeanBarName' TEXT,
            'REF' TEXT,
            'ReviewDate' TEXT,
            'CocoaPercent' REAL,
            'CompanyLocationId' INTEGER,
            'Rating' REAL,
            'BeanType' TEXT,
            'BroadBeanOrigin' INTEGER
        );
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT,
            'Alpha3' TEXT,
            'EnglishName' TEXT,
            'Region' TEXT,
            'Subregion' TEXT,
            'Population' INTEGER,
            'Area' REAL
        );
    '''
    cur.execute(statement)
    conn.commit()
    conn.close()


def open_csv(name):
    ifile  = open(name, "r",encoding="utf8", errors='ignore')
    read = csv.reader(ifile)
    list_of_lists = []
    for row in read:
        list_of_lists.append(row)
    return list_of_lists




def insert_stuff_bars(name):
    list_of_lists = open_csv(name)
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    for obs in list_of_lists[1:]:
        insertion = (None,obs[0],obs[1],obs[2],obs[3],float(obs[4].strip("%")),obs[5],obs[6],obs[7],obs[8])
        statement = 'INSERT INTO "Bars"'
        statement += 'VALUES (?,?,?,?,?,?,?,?,?,?)'
        cur.execute(statement,insertion)

    conn.commit()
    conn.close()

def open_json(file_name):
    json_file = open(file_name, 'r')
    file_contents = json_file.read()
    json_dict_list = json.loads(file_contents)
    json_file.close()
    return json_dict_list


def insert_stuff_countries(name):
    json_dict_list = open_json(name)
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    for obs in json_dict_list:
        insertion = (None,obs['alpha2Code'],obs['alpha3Code'],obs['name'],obs['region'],obs['subregion'],obs['population'],obs['area'])
        statement = 'INSERT INTO "Countries"'
        statement += 'VALUES (?,?,?,?,?,?,?,?)'
        cur.execute(statement,insertion)

    conn.commit()
    conn.close()


def update_bean_ids():
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    statement = "INSERT INTO Countries ('Alpha2','Alpha3','EnglishName','Region','Subregion','Population','Area') VALUES('Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown')"
    cur.execute(statement)


    statement = "SELECT Id, EnglishName FROM Countries"
    cur.execute(statement)
    tup_list = cur.fetchall()

    country_dict = {}
    for tup in tup_list:
        country_dict[tup[1]] = tup[0]
    # country_dict['Unknown'] = 'Unknown'
    for country in country_dict.keys():
        company_id = country_dict[country]
        cur.execute("UPDATE Bars SET CompanyLocationId = ? WHERE CompanyLocationId = ?", (company_id, country))
        cur.execute("UPDATE Bars SET BroadBeanOrigin = ? WHERE BroadBeanOrigin = ?", (company_id, country))

    ### UPDATE ID for COUNTRIES: UNKNOWN ###

    conn.commit()
    conn.close()
    return None


###############################################################################
###############################################################################
########################### UPDATE OR INIT DB #################################
###############################################################################
###############################################################################

if len(sys.argv) >1 and sys.argv[1] == '--init':
    print('Deleting db and starting over from scratch.')
    init_db()
elif len(sys.argv) >1 and sys.argv[1] == 'bars' and sys.argv[2] == 'add':
    print('Adding bars to db.')
    insert_stuff_bars(BARSCSV)
elif len(sys.argv) >1 and sys.argv[1] == 'countries' and sys.argv[2] == 'add':
    print('Adding countries to db.')
    insert_stuff_countries(COUNTRIESJSON)
elif len(sys.argv) >1 and sys.argv[1] == 'update':
    print('Updating db.')
    update_bean_ids()
else:
    print('Leaving the DB alone.')


# Part 2: Implement logic to process user commands
def process_command(user_input):
    ### returns a list of tuples representing records matching that query
    ### 4 main commands:
    user_list = user_input.split(" ")

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

###############################################################################
###############################################################################
################################## BARS #######################################
###############################################################################
###############################################################################

    '''
    bars
        Description: Lists chocolate bars, according the specified parameters.
        Parameters:
            sellcountry=<alpha2> | sourcecountry=<alpha2> | sellregion=<name> | sourceregion=<name> [default: none]
                Description: Specifies a country or region within which to limit the results, and also specifies whether to limit by the seller (or manufacturer) or by the bean origin source.
        ratings | cocoa [default: ratings]
            Description: Specifies whether to sort by rating or cocoa percentage
        top=<limit> | bottom=<limit> [default: top=10]
            Description: Specifies whether to list the top <limit> matches or the bottom <limit> matches.
    '''
    if user_list[0] == 'bars':
        param_defaults = {'sort':'ratings','top':10}
        param_diction = {}
        for input in user_list[1:]:
            # print(input)
            if '=' in input:
                front_back = input.split('=')
                param_diction[front_back[0]]=front_back[1]
                # print('\t',(front_back[0],front_back[1]))
            else:
                param_diction['sort']=input
                # print('\t',('sort',input))
        # print(param_diction)
        if 'sort' not in param_diction.keys():
            param_diction['sort']=param_defaults['sort']
            # print('\t',('sort',param_defaults['sort']))
        if 'top' not in param_diction:
            if 'bottom' not in param_diction:
                param_diction['top']=param_defaults['top']
            # print('\t',('top',param_defaults['top']))
        # print(param_diction)


        #setting up parameters for SQL
        arguments = []
        for key in param_diction.keys():
            arguments.append((key,param_diction[key]))
        input_sql_conversion = {'sellcountry':'sell.alpha2','sellregion':'sell.Region','sourcecountry':'source.alpha2','sourceregion':'source.Region','cocoa':'Bars.CocoaPercent','ratings':'Bars.Rating'}


        start = "SELECT SpecificBeanBarName, Company, sell.EnglishName, Rating, CocoaPercent, source.EnglishName FROM Bars "
        # if 'sellcountry' or 'sellregion' in param_diction.keys():
        #     join = "JOIN Countries ON Bars.CompanyLocationId = Countries.Id "
        # elif 'sourcecountry' or 'sourceregion' in param_diction.keys():
        #     join = "JOIN Countries ON Bars.BroadBeanOrigin = Countries.Id"
        join_sell = "JOIN Countries as sell ON Bars.CompanyLocationId = sell.Id "
        join_source = "JOIN Countries as source ON Bars.BroadBeanOrigin = source.Id "
        where_clause = ''
        addtl_where = ''
        sort = ''
        t_b = ''
        argument_list = []
        for item in arguments:
            if item[0] == 'sort':
                sort = 'ORDER BY ' + input_sql_conversion[item[1]]
                if item[1] == 'cocoa':
                    addtl_where = 'Bars.CocoaPercent <> "100%" '
            elif item[0] == 'top':
                t_b = ' DESC limit ' + str(item[1])
            elif item[0] == 'bottom':
                t_b = ' ASC limit ' + str(item[1])
            else:
                where_clause = ' WHERE ' +input_sql_conversion[item[0]]+'=? '
                argument_list.append(item[1])
        if where_clause == '' and addtl_where !='':
            statement = start+join_sell+join_source+' WHERE '+addtl_where+sort+t_b
        elif where_clause != '' and addtl_where !='':
            statement = start+join_sell+join_source+where_clause+'AND '+addtl_where+sort+t_b
        else:
            statement = start+join_sell+join_source+where_clause+addtl_where+sort+t_b
        argument = tuple(argument_list)
        # print(statement)
        cur.execute(statement,argument)
        tup_list = cur.fetchall()
        return tup_list

###############################################################################
###############################################################################
################################ COMPANIES ####################################
###############################################################################
###############################################################################
    elif user_list[0] == 'companies':
        param_defaults = {'sort':'ratings','top':10}
        param_diction = {}
        for input in user_list[1:]:
            # print(input)
            if '=' in input:
                front_back = input.split('=')
                param_diction[front_back[0]]=front_back[1]
                # print('\t',(front_back[0],front_back[1]))
            else:
                param_diction['sort']=input
                # print('\t',('sort',input))
        # print(param_diction)
        if 'sort' not in param_diction.keys():
            param_diction['sort']=param_defaults['sort']
            # print('\t',('sort',param_defaults['sort']))
        if 'top' not in param_diction:
            if 'bottom' not in param_diction:
                param_diction['top']=param_defaults['top']
            # print('\t',('top',param_defaults['top']))
        # print(param_diction)

        #setting up parameters for SQL
        arguments = []
        for key in param_diction.keys():
            arguments.append((key,param_diction[key]))
        input_sql_conversion = {'country':'sell.alpha2','region':'sell.Region','cocoa':'Bars.CocoaPercent','ratings':'Bars.Rating','bars_sold':'Bars.SpecificBeanBarName'}

        start = "SELECT Company, sell.EnglishName"
        from_clause = " FROM Bars "
        join_sell = "JOIN Countries as sell ON Bars.CompanyLocationId = sell.Id "
        # join_source = "JOIN Countries as source ON Bars.BroadBeanOrigin = source.Id "
        where_clause = ''
        sort = ''
        t_b = ''
        agg = ''
        grouping = 'GROUP BY Company '
        having_count = 'HAVING COUNT(*) >4 '
        argument_list = []
        for item in arguments:
            if item[0] == 'sort':
                if item[1] == 'bars_sold':
                    agg = ', count(*)'
                    sort = 'ORDER BY count(*) '
                elif item[1] == 'ratings':
                    agg = ', ROUND(AVG(Bars.Rating),1) AS Rating'
                    sort = 'ORDER BY avg(Bars.Rating) '
                elif item[1] == 'cocoa':
                    agg = ', avg(Bars.CocoaPercent) '
                    sort = 'ORDER BY avg(Bars.CocoaPercent) '
            elif item[0] == 'top':
                t_b = ' DESC limit ' + str(item[1])
            elif item[0] == 'bottom':
                t_b = ' ASC limit ' + str(item[1])

            else:
                where_clause = ' WHERE ' +input_sql_conversion[item[0]]+'=? '
                argument_list.append(item[1])
        statement = start+agg+from_clause+join_sell+where_clause+grouping+having_count+sort+t_b
        argument = tuple(argument_list)
        # print(statement)
        cur.execute(statement,argument)
        tup_list = cur.fetchall()
        return tup_list


###############################################################################
###############################################################################
################################ COUNTRIES ####################################
###############################################################################
###############################################################################
    elif user_list[0] == 'countries':
        param_defaults = {'sort':'ratings','top':10, 'join':'sellers'}
        param_diction = {}
        for input in user_list[1:]:
            # print(input)
            if '=' in input:
                front_back = input.split('=')
                param_diction[front_back[0]]=front_back[1]
                # print('\t',(front_back[0],front_back[1]))
            elif input == 'sources':
                param_diction['join']=input
            else:
                param_diction['sort']=input
                # print('\t',('sort',input))
        # print(param_diction)
        if 'sort' not in param_diction.keys():
            param_diction['sort']=param_defaults['sort']
            # print('\t',('sort',param_defaults['sort']))
        if 'top' not in param_diction:
            if 'bottom' not in param_diction:
                param_diction['top']=param_defaults['top']
            # print('\t',('top',param_defaults['top']))
        if 'join' not in param_diction:
                param_diction['join']=param_defaults['join']
        # print(param_diction)

        #setting up parameters for SQL
        arguments = []
        for key in param_diction.keys():
            arguments.append((key,param_diction[key]))
        input_sql_conversion = {'country':'Countries.alpha2','region':'Countries.Region','cocoa':'Bars.CocoaPercent','ratings':'Bars.Rating','bars_sold':'Bars.SpecificBeanBarName'}
        start = "SELECT Countries.EnglishName, Region"
        grouping = 'GROUP BY CompanyLocation '
        from_clause = " FROM Bars "
        if param_diction['join'] == 'sellers':
            join = "JOIN Countries ON Bars.CompanyLocationId = Countries.Id "
            grouping = 'GROUP BY CompanyLocationId '

        elif param_diction['join'] == 'sources':
            join = "JOIN Countries ON Bars.BroadBeanOrigin = Countries.Id "
            grouping = 'GROUP BY BroadBeanOrigin '
        where_clause = ''
        exclude = 'Countries.EnglishName != "Côte d\'Ivoire" '
        sort = ''
        t_b = ''
        agg = ''
        #having_count = ''
        having_count = 'HAVING COUNT(*) >=4 '
        argument_list = []
        for item in arguments:
            if item[0] == 'sort':
                if item[1] == 'bars_sold':
                    agg = ', count(*)'
                    sort = 'ORDER BY count(*) '
                elif item[1] == 'ratings':
                    agg = ', ROUND(AVG(Bars.Rating),1) AS Rating'
                    sort = 'ORDER BY avg(Bars.Rating) '
                elif item[1] == 'cocoa':
                    agg = ', avg(Bars.CocoaPercent) '
                    sort = 'ORDER BY avg(Bars.CocoaPercent) '
            elif item[0] == 'top':
                t_b = ' DESC limit ' + str(item[1])
            elif item[0] == 'bottom':
                t_b = ' ASC limit ' + str(item[1])
            elif item[0] == 'join':
                continue
            else:
                where_clause = 'AND '+input_sql_conversion[item[0]]+'=? '
                argument_list.append(item[1])
        statement = start+agg+from_clause+join+' WHERE '+exclude+where_clause+grouping+having_count+sort+t_b
        argument = tuple(argument_list)
        # print(statement)
        cur.execute(statement,argument)
        tup_list = cur.fetchall()
        return tup_list

###############################################################################
###############################################################################
################################## REGIONS ####################################
###############################################################################
###############################################################################
    elif user_list[0] == 'regions':
        param_defaults = {'sort':'ratings','top':10, 'join':'sellers'}
        param_diction = {}
        for input in user_list[1:]:
            # print(input)
            if '=' in input:
                front_back = input.split('=')
                param_diction[front_back[0]]=front_back[1]
                # print('\t',(front_back[0],front_back[1]))
            elif input == 'sources':
                param_diction['join']=input
            else:
                param_diction['sort']=input
                # print('\t',('sort',input))
        # print(param_diction)
        if 'sort' not in param_diction.keys():
            param_diction['sort']=param_defaults['sort']
            # print('\t',('sort',param_defaults['sort']))
        if 'top' not in param_diction:
            if 'bottom' not in param_diction:
                param_diction['top']=param_defaults['top']
            # print('\t',('top',param_defaults['top']))
        if 'join' not in param_diction:
                param_diction['join']=param_defaults['join']
        # print(param_diction)

        #setting up parameters for SQL
        arguments = []
        for key in param_diction.keys():
            arguments.append((key,param_diction[key]))
        input_sql_conversion = {'country':'Countries.alpha2','region':'Countries.Region','cocoa':'Bars.CocoaPercent','ratings':'Bars.Rating','bars_sold':'Bars.SpecificBeanBarName'}
        join = "JOIN Countries ON Bars.CompanyLocationId = Countries.Id "
        start = "SELECT Region"
        grouping = 'GROUP BY Region '
        from_clause = " FROM Bars "
        if param_diction['join'] == 'sellers':
            join = "JOIN Countries ON Bars.CompanyLocationId = Countries.Id "
            start = "SELECT Region"
            grouping = 'GROUP BY Region '

        elif param_diction['join'] == 'sources':
            join = "JOIN Countries ON Bars.BroadBeanOrigin = Countries.Id "
            start = "SELECT Region"
            grouping = 'GROUP BY Region '
        where_clause = " WHERE Countries.Id != 251 "
        sort = ''
        t_b = ''
        agg = ''
        having_count = 'HAVING COUNT(*) >4 '
        argument_list = []
        for item in arguments:
            if item[0] == 'sort':
                if item[1] == 'bars_sold':
                    agg = ', count(*)'
                    sort = 'ORDER BY count(*) '
                elif item[1] == 'ratings':
                    #agg = ', avg(Bars.Rating) '
                    agg = ', ROUND(AVG(Bars.Rating),1) AS Rating'
                    sort = 'ORDER BY avg(Bars.Rating) '
                elif item[1] == 'cocoa':
                    agg = ', avg(Bars.CocoaPercent) '
                    sort = 'ORDER BY avg(Bars.CocoaPercent) '
            elif item[0] == 'top':
                t_b = ' DESC limit ' + str(item[1])
            elif item[0] == 'bottom':
                t_b = ' ASC limit ' + str(item[1])
            elif item[0] == 'join':
                continue
            else:
                where_clause = ' WHERE ' +input_sql_conversion[item[0]]+'=? '
                argument_list.append(item[1])
        statement = start+agg+from_clause+join+where_clause+grouping+having_count+sort+t_b
        # print(statement)
        argument = tuple(argument_list)
        cur.execute(statement,argument)
        tup_list = cur.fetchall()
        return tup_list


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while True:
        response = input("Enter a command, type 'help' for more info, or type 'exit' to end the program: ")
        if response == 'exit':
            print('Exiting...')
            break
        try:
            tuple_response = process_command(response)
            # print(tuple_response)
            row_num = 0
            table_spacing = [0,0,0,0,0,0,0,0,0,0,0,0]
            param_strings = []
            for tup in tuple_response:
                for i in range(0,len(tup)):
                    max_len = table_spacing[i]
                    if type(tup[i]) == type('string'):
                        if len(tup[i]) > max_len:
                            table_spacing[i] = len(tup[i])
                    else:
                        table_spacing[i] = 5
            for i in table_spacing:
                param_strings.append('{0: <'+str(i)+'}')

            for tup in tuple_response:
                print_list = []
                for i in range(0,len(tup)):
                    if type(tup[i]) == type(5.0) and tup[i] >5:
                        print_list.append(param_strings[i].format(str(tup[i])+"%"))
                    else:
                        print_list.append(param_strings[i].format(tup[i]))
                print(*print_list, sep=' ')
            print('\n')
        except:
            print("Command not recognized: "+response+'\n')

        if response == 'help':
            print(help_text)
            continue

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    init_db()
    print('Deleting db and starting over from scratch.')

    insert_stuff_bars(BARSCSV)
    print('Adding bars to db.')

    insert_stuff_countries(COUNTRIESJSON)
    print('Adding countries to db.')

    update_bean_ids()
    print('Linking tables in db.')

    interactive_prompt()
