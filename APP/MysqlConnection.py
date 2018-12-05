import mysql.connector
from mysql.connector import Error


class MysqlConnection(object):
    __instance = None
    __host = None
    __user = None
    __password = None
    __database = None
    __session = None
    __connection = None

    def __init__(self, host, user, password, database):
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database

    def _open(self):
        try:
            con = mysql.connector.connect(host=self.__host,
                                          user=self.__user,
                                          password=self.__password,
                                          database=self.__database,
                                          connection_timeout=180)

            if con.is_connected():
                # db_info = con.get_server_info()
                # print("Connected to MySQL database... MySQL Server version on ", db_info)
                self.__connection = con
                self.__session = con.cursor(buffered=True)

                # global connection timeout arguments
                global_connect_timeout = 'SET GLOBAL connect_timeout=180'
                global_wait_timeout = 'SET GLOBAL connect_timeout=180'
                global_interactive_timeout = 'SET GLOBAL connect_timeout=180'
                self.__session.execute(global_connect_timeout)
                self.__session.execute(global_wait_timeout)
                self.__session.execute(global_interactive_timeout)

        except Error as e:
            print("Error while connecting to MySQL", e)

    def _close(self):
        # closing database connection.
        if self.__connection.is_connected():
            self.__session.close()
            self.__connection.close()
            # print("MySQL connection is closed")

    def select(self, query):
        query = "SELECT "+query
        # print(query)
        self._open()
        self.__session.execute(query)
        self.__connection.commit()
        result = self.__session.fetchall()
        self._close()
        # print("connection closed")
        return result

    def delete(self, table, column, index):
        for each in index:
            query = "DELETE FROM %s WHERE %s=%d" % (table, column, each)
            self._open()
            self.__session.execute(query)
            self.__connection.commit()
            self._close()
        return ' Registros deletados da produção '

    def insert(self, table, query, values):
        list_id = []
        insert = "INSERT INTO "+table+query
        for each in values:
            self._open()
            self.__session.execute(insert, each)
            self.__connection.commit()
            inserted_id = self.__session.lastrowid
            self._close()
            list_id.append(inserted_id)
        # print("connection closed")
        return list_id

    def show(self, query):
        self._open()
        self.__session.execute(query)
        self.__connection.commit()
        result = self.__session.fetchall()
        self._close()
        return result

    def execute(self, query):
        self._open()
        self.__session.execute(query)
        self.__connection.commit()
        self._close()
