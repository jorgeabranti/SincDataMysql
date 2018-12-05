from APP.SincDataTimeCheck import SincDataChecks


class SincDataProcess:
    __table_name = None
    __order_by_date = None
    __days_in_prod = None
    __connection_prod = None
    __connection_bh = None
    __time_start = None
    __time_end = None

    def __init__(self, table_name, order_by_date, pk_table, days_in_prod, connection_prod, connection_bh, time_start,
                 time_end, log_data):
        self.__table_name = table_name
        self.__order_by_date = order_by_date
        self.__pk_table = pk_table
        self.__days_in_prod = days_in_prod
        self.__connection_prod = connection_prod
        self.__connection_bh = connection_bh
        self.__time_start = time_start
        self.__time_end = time_end
        self.__log_data = log_data

    def __check_table_exist(self):
        table_name = self.__table_name
        connection_prod = self.__connection_prod
        connection_bh = self.__connection_bh
        table_bh = connection_bh.select(" COUNT(1) FROM information_schema.tables WHERE table_schema=DATABASE() AND "
                                        "table_name = '" + table_name + "'")
        if table_bh[0][0] == 0:
            create_table = connection_prod.show("SHOW CREATE TABLE " + table_name)
            connection_bh.execute(create_table[0][1])

    def process_data(self):

        connection_prod = self.__connection_prod
        connection_bh = self.__connection_bh
        table_name = self.__table_name
        days_in_prod = self.__days_in_prod
        order_by_date = self.__order_by_date
        pk_table = self.__pk_table
        __optimize_table = False

        self.__check_table_exist()
        self.__log_data.write_log("Efetuando busca de registros para cópia... tabela "+table_name)

        __column_prod = connection_prod.select(" COLUMN_NAME FROM information_schema.columns WHERE "
                                               "table_schema=DATABASE() AND table_name='" + table_name +
                                               "' ORDER BY COLUMN_NAME")
        __column_bh = connection_bh.select(" COLUMN_NAME FROM information_schema.columns WHERE "
                                           "table_schema=DATABASE() AND table_name='" + table_name +
                                           "' ORDER BY COLUMN_NAME")
        if all(elem in __column_prod for elem in __column_bh):
            while SincDataChecks(self.__time_start, self.__time_end)._process_time() is True:
                __var_select = ""
                __column_insert = "("
                __values_insert = []
                __values_delete = []
                __cont = 1
                __value_entered = ""
                for c1 in __column_prod:
                    if __cont < len(__column_prod):
                        __column = str(c1[0]) + ","
                    else:
                        __column = str(c1[0])
                    __var_select = __var_select + __column
                    __column_insert = __column_insert + __column
                    __cont += 1

                __con_bh = connection_bh.select(" MAX("+order_by_date+") FROM " + table_name)
                __max_date_bh = __con_bh[0][0]
                self.__log_data.write_log("MAX DATE NA BH "+str(__max_date_bh)+" tabela "+table_name)
                if not __max_date_bh:
                    __max_date_bh = 0
                __con_prod = connection_prod.select(str(__var_select) + " FROM " + table_name + " WHERE " +
                                                    order_by_date+" BETWEEN '" +
                                                    str(__max_date_bh) + "' AND CURDATE() - INTERVAL " +
                                                    days_in_prod +
                                                    " DAY ORDER BY "+order_by_date+"  LIMIT 1000")
                if __con_prod:
                    self.__log_data.write_log("Copiando "+str(len(__con_prod))+" registros... tabela "+table_name)
                    for row in __con_prod:
                        __values = []
                        __cont2 = 1
                        for t in row:
                            __value_entered = ', '.join(['%s'] * len(row))
                            __values.append(t)
                            __cont2 += 1
                        __values_insert.append(__values)
                    result = connection_bh.insert(table_name, __column_insert + ") VALUES (" + __value_entered +
                                                  ")",
                                                  __values_insert)
                    result_delete = connection_prod.delete(table_name, pk_table, result)
                    self.__log_data.write_log(str(len(result))+result_delete+" tabela "+table_name)
                    __optimize_table = True
                else:
                    self.__log_data.write_log("Não tenho nada para importar tabela "+table_name)
                    if __optimize_table is True:
                        self.__log_data.write_log("Iniciando otimização da tabela... "+table_name)
                        connection_prod.execute("OPTIMIZE TABLE " + table_name)
                        __optimize_table = False
                        self.__log_data.write_log("Tabela "+table_name+" otimizada")
                    break
        else:
            self.__log_data.write_log("Divergência de colunas!!! tabela "+table_name)
