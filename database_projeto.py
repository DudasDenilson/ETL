import psycopg2


def con_database():
    con = None
    try:
        con = psycopg2.connect(host='localhost', database='dwteste', user='postgres', password='postgres')
        return con
    except Exception as e:
        print('Problem to connect database ' + str(e))
    return con


def create_table(sql_create, table_name):
    try:
        con = con_database()
        cursor = con.cursor()
        cursor.execute(sql_create)
        con.commit()
        print('Table Created' + table_name)
        con.close()
    except Exception as e:
        print('Problem to create table  ' + table_name + str(e))
        drop_table()
        create_table(sql_create, table_name)


def drop_table(table_name):
    SQL_DROP = 'drop table ' + table_name + ' cascade ;'
    try:
        con = con_database()
        cursor = con.cursor()
        cursor.execute(SQL_DROP)
    except Exception as e:
        print('Problem to drop table  ' + table_name + str(e))
