import csv
import os
import mysql.connector
from mysql.connector import errorcode

# Setting up mysql connection settings.
cnx = mysql.connector.connect(user='root',
                              password='root',
                              #host='127.0.0.1',
                              #database='movies',
                              unix_socket= '/Applications/MAMP/tmp/mysql/mysql.sock',
                              )

DB_NAME = 'Movies'

cursor = cnx.cursor()

# Creating the database.
def create_database(cursor, DB_NAME):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

# Creating table awards if not existed.
# We need award_id because none of the values are unique
def create_table_awards(cursor):
    creat_awards = "CREATE TABLE `Awards` (" \
                 "  `Award_id` int(64) NOT NULL AUTO_INCREMENT," \
                 "  `Year` varchar(24) NOT NULL," \
                 "  `Ceremony` int(3)," \
                 "  `Award` TINYTEXT," \
                 "  `Winner` BOOLEAN," \
                 "  `Name` TINYTEXT," \
                 "  `Film` TINYTEXT," \
                 "  PRIMARY KEY (`Award_id`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating table awards: ")
        cursor.execute(creat_awards)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Creating table IMDB if not existed.
def create_table_IMDB(cursor):
    creat_IMDB = "CREATE TABLE `IMDB` (" \
                 "  `movie_id` int(5) NOT NULL AUTO_INCREMENT," \
                 "  `Title` TINYTEXT NOT NULL," \
                 "  `Year` int(4)," \
                 "  `Rated` varchar(64)," \
                 "  `Runtime` int(12)," \
                 "  `Genre` TINYTEXT," \
                 "  `Director` MEDIUMTEXT," \
                 "  `Writer` MEDIUMTEXT," \
                 "  `Actors` TINYTEXT," \
                 "  `Plot` LONGTEXT," \
                 "  `Language` TINYTEXT," \
                 "  `Country` MEDIUMTEXT," \
                 "  `ImdbRating` FLOAT," \
                 "  `BoxOffice` int," \
                 "  `Production` varchar(32)," \
                 "  PRIMARY KEY (`movie_id`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating table IMDB: ")
        cursor.execute(creat_IMDB)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

def create_table_streams(cursor):
    creat_streams = "CREATE TABLE `streams` (" \
                "  `id` int(12) AUTO_INCREMENT," \
                "  `Title` TINYTEXT," \
                "  `Year` int(12)," \
                "  `Netflix` int(1)," \
                "  `Hulu` int(1)," \
                "  `Prime` int(1)," \
                "  `Disney` int(1)," \
                "  `Runtime` int(12)," \
                "  PRIMARY KEY (`id`)" \
                ") ENGINE=InnoDB"

    try:
        print("Creating table streams: ")
        cursor.execute(creat_streams)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")
# Inserting data from csv to table awards.
def insert_into_awards(cursor):
    # First we read the data then we insert it into a tuple then we add it as queries to a list (insert_sql).
    path = os.getcwd() + "/data/AWARDS.csv"
    with open(path, encoding="utf-8") as csv_file:
        csvfile = csv.reader(csv_file, delimiter=';')
        insert_sql = []
        for row in csvfile:
            value = (row[0], row[1], row[2], row[3], row[4], row[5])
            insert_sql.append(((str(value).replace("'NULL'","NULL")).replace("'FALSE'", "FALSE")).replace("'TRUE'", "TRUE")) # Because my database doesnt interpret 'NA' as NULL
        
    upper_query = "INSERT INTO Awards " + "(Year, Ceremony, Award, Winner, Name, Film)"
    del insert_sql[0] # delete the titles: (name,rotation,...)
    for i in range(len(insert_sql)):
        insert_sql[i] = upper_query + "VALUES " + insert_sql[i] + ";"

    for query in insert_sql:
        try:
            #print("SQL query {}: ".format(query), end='')
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            cnx.commit()
            #print("OK")

# Inserting data from csv to table IMDB.
def insert_into_IMDB(cursor):
    path = os.getcwd() + "/data/IMDB.csv"
    with open(path, encoding="utf-8") as csv_file:
        csvfile = csv.reader(csv_file, delimiter=';')
        insert_sql = []
        for row in csvfile:
            value = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12])
            insert_sql.append((str(value)).replace("'NULL'", "NULL"))

    upper_query = "INSERT INTO IMDB " + "(Title, Year, Rated, Runtime, Genre, Director, Writer, Actors, Plot, Language, Country, imdbRating, BoxOffice)" # ('name','class'..) ==> (name,class..)
    del insert_sql[0]
    for i in range(len(insert_sql)):
        insert_sql[i] = upper_query + "VALUES " + insert_sql[i] + ";"

    for query in insert_sql:
        try:
            #print("SQL query {}: ".format(query), end='')
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)

        else:
            cnx.commit()
            #print("OK")

def insert_into_streams(cursor):
    path = os.getcwd() + "/data/STREAMS.csv"
    with open(path, encoding="utf-8-sig") as csv_file:
        csvfile = csv.reader(csv_file, delimiter=';')
        insert_sql = []
        for row in csvfile:
            value = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])
            insert_sql.append((str(value).replace("'N/A'", "NULL"))) # because my database doesnt interpret NA as NULL

    upper_query = "INSERT INTO streams " + "(Title, Year, Netflix, Hulu, Prime, Disney, Runtime)" # ('name','class'..) ==> (name,class..)
    del insert_sql[0] # delete the titles: (name,class,...)
    for i in range(len(insert_sql)):
        insert_sql[i] = upper_query + "VALUES " + insert_sql[i] + ";"

    for query in insert_sql:
        try:
            #print("SQL query {}: ".format(query), end='')
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            cnx.commit()
            #print("OK")

def view_tables(cursor):
    query = "SELECT Country from {} WHERE Number>5;".format("netflix_country")
    cursor.execute(query)
    print ("\nNetflix: ")
    for (Country) in cursor:
        print(Country[0])

    query = "SELECT Country from {} WHERE Number>5;".format("prime_country")
    cursor.execute(query)
    print ("\nAmazon Prime: ")
    for (Country) in cursor:
        print(Country[0])

    query = "SELECT Country from {} WHERE Number>5;".format("hulu_country")
    cursor.execute(query)
    print ("\nhulu Prime: ")
    for (Country) in cursor:
        print(Country[0])

    query = "SELECT Country from {} WHERE Number>5;".format("disney_country")
    cursor.execute(query)
    print ("\nDisney+: ")
    for (Country) in cursor:
        print(Country[0])
    
# User interface with options from 1-5.
def user_interface(cursor, key):
    if key =='1':
        name = str(input("Enter the name of the movie: "))
        query = ("SELECT Actors, Director "\
            "FROM imdb " \
        "WHERE Title='{}'".format(name))
        cursor.execute(query)
        for (Actors, Director) in cursor:
            print("Actors: {}\nDirector: {}".format(Actors, Director))
    
    elif key =='2':
        name = str(input("Enter the name of the movie: "))
        query = ("SELECT Netflix, Hulu, Prime, Disney "\
            "FROM streams " \
        "WHERE Title='{}'".format(name))
        cursor.execute(query)
        for (Netflix,Hulu,Prime,Disney) in cursor:
            if ("{}".format(Netflix)=='1'):
                print("Available on Netflix")
            if ("{}".format(Hulu)=='1'):
                print("Available on Hulu")
            if ("{}".format(Prime)=='1'):
                print("Available on Amazon Prime")
            if ("{}".format(Disney)=='1'):
                print("Available on Disney+")

    elif key == '3':
        name = str(input("Enter the name of the Actor: "))
        query = ("SELECT Award, Year, Film "\
            "FROM awards " \
        "WHERE Name='{}'" \
        " AND Winner=TRUE".format(name))
        cursor.execute(query)
        for (Award, Year, Film) in cursor:
            print("Award: {}\nYear: {}\nFor movie: {}".format(Award, Year, Film))

    elif key == '4':
        i=0
        query = ("SELECT Country, COUNT(Country) as Number_of_movies, SUM(BoxOffice) as summa FROM imdb" \
            " GROUP BY imdb.Country HAVING Number_of_movies>=20 and summa>0" \
                " ORDER BY summa DESC;")
        cursor.execute(query)
        for (Country, Number_of_movies, summa) in cursor:
                {
                    print("Country{:<15} Number of movies:{:<15} Revenue:{}$".format(Country, Number_of_movies, summa))
                }
    elif key == '5':
        query_net = "CREATE VIEW Netflix_Country AS" \
                    " SELECT Country, COUNT(Country) AS Number" \
                    " FROM imdb" \
                    " JOIN streams ON imdb.Title = streams.Title AND streams.Netflix=1" \
                    " GROUP BY country" \
                        " ORDER BY Number DESC"

        query_pri = "CREATE VIEW Prime_Country AS" \
                    " SELECT Country, COUNT(Country) AS Number" \
                    " FROM imdb" \
                    " JOIN streams ON imdb.Title = streams.Title AND streams.Prime=1" \
                    " GROUP BY country" \
                        " ORDER BY Number DESC"

        query_hulu = "CREATE VIEW Hulu_Country AS" \
                    " SELECT Country, COUNT(Country) AS Number " \
                    " FROM imdb" \
                    " JOIN streams ON imdb.Title = streams.Title AND streams.Hulu=1" \
                    " GROUP BY country" \
                        " ORDER BY Number DESC"

        query_des = "CREATE VIEW Disney_Country AS" \
                    " SELECT Country, COUNT(Country) AS Number" \
                    " FROM imdb" \
                    " JOIN streams ON imdb.Title = streams.Title AND streams.Disney=1" \
                    " GROUP BY country" \
                        " ORDER BY Number DESC"

        try:
            cursor.execute(query_net)
            cursor.execute(query_pri)
            cursor.execute(query_hulu)
            cursor.execute(query_des)
            view_tables(cursor)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                view_tables(cursor)
                

try:
    cursor.execute("USE {}".format(DB_NAME)) # We try to use database to check if it exists.
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))

    create_database(cursor, DB_NAME)
    print("Database {} created successfully.".format(DB_NAME))
    cnx.database = DB_NAME
    create_table_awards(cursor)
    insert_into_awards(cursor)

    create_table_IMDB(cursor)
    insert_into_IMDB(cursor)

    create_table_streams(cursor)
    insert_into_streams(cursor)


# While loop of asking the user to choose from 1-5.
while (True):
    key = str(input("1. Search for Actors and directors to a specific movie.\n2. See which platform a specific movie is available on.\n3. See which awards did an actor won/or if he didnt won any.\n4. List of  most earning countries from making movies and how much did they make in USD.\n5. List out the top countries on each platform.\n"))
    user_interface(cursor,key)
    input("Press Enter to go back to main-menu...")

cursor.close()
cnx.close()

