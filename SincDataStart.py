from APP.SincDataThreading import SincDataThreading
"""                                !!!LEIA COM ATENÇÃO!!!  
*tables(string): especifica as tabelas que devem ter os dados migrados
*table_name(string): nome da tabela a ter os dados migrados
*order_by_date(string): nome da coluna 'date' utilizada nas cláusulas order by e para controle do período de cópia
*pk_table(string): nome da coluna chave primária da tabela
*connection_prod(string): dados para conexão com a base de produção
*connection_bh(string): dados para conexão com a base histórica
*time_start(string): horário para início da cópia
*time_end(string): horário para fim da cópia
*days_in_prod(string): quantidade de dias para manter na base de produção
*days_keep_log(int): quantidade de dias para manter os arquivos de log
"""
__imports = {"tables": [
                {"table_name": "chamadas", "order_by_date": "calldate", "pk_table": "id_chamada"},
                {"table_name": "chamadas_2", "order_by_date": "calldate", "pk_table": "id_chamada"},
                {"table_name": "chamadas_3", "order_by_date": "calldate", "pk_table": "id_chamada"}
            ],
            "connection_prod": {"host": "localhost", "user": "system", "password": "12345", "database": "dev_db_2"},
            "connection_bh": {"host": "localhost", "user": "system", "password": "12345", "database": "dev_db"},
            "time_start": "18:22:00",
            "time_end": "07:00:00",
            "days_in_prod": "2",
            "days_keep_log": 1}

SincDataThreading(__imports).execute_import()
