import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    srv_ip = str(input("IP адресс подключения?\t"))
    srv_port = str(input("Порт подключения?\t"))
    name_bd = str(input("К какой базе хотите подключиться?\t"))
    connnect_db = psycopg2.connect(
    host = srv_ip,
    port = srv_port,
    database = name_bd,
    user = "postgres",
    password = "")
    cursor = connnect_db.cursor()
    print(f'Вы успешно подключились к базе данных {name_bd} на хосте {srv_ip}')
except (Exception, Error) as error:
    print(f'Ошибка на этапе подключения к базе данных {name_bd} на хосте {srv_ip}: ', error)

exit_code = 0
while exit_code == 0:
    print("""
\t1 - Показать информацию по САООГ
\t2 - Размер базы данных
\t0 - Завершить работу и закрыть соединение""")
    answer = int(input("Что делаем?  "))
    if answer == exit_code:
        cursor.close()
        connnect_db.close()
        print('До скорой встречи!')
        break
    elif answer == 1:
        while True:
            print("""
\tВведите название ОВУ для отображения колличества лицензий например 'урог'
\tДля отображения всего списка ову\лицензии нажмите '1'
\tДля отображения суммы всех лицензий нажмите '2'
\tДля отображения колличества организаций нажмите '3'
\tДля отображения списка доменов в данной БД нажмите '4'
\tДля отображения суммы всех доменов нажмите '5'
\tВернуться назад нажмите '0' """)
            name_ovu = str(input("Что делаем?  ")).lower()
            list = name_ovu.split()
            if '0' in list:
                print("Идем назад")
                break
            else:
                sql_query = '''
select c."Name", count(*)
from "SAOOG"."SystemUsers" as a
join "SAOOG"."Подразделения" as b on a."Отдел"=b."RecordId"
join "SAOOG"."Организации" as c on c."RecordId"=b."Root"
where a."Фиктивный"='0' 
and a."Name"!='SYSTEM1' 
and a."Name"!='System' 
and a."Name"!='Admin'
and a."RecordId" !='00000000-0000-0000-0000-000000000000'
and c."Name" ilike any(%s)
group by c."Name"
'''
                sql_query1 = '''
select c."Name" as "ОВУ", count(*) as "Сколько лицензий"
from "SAOOG"."SystemUsers" as a
join "SAOOG"."Подразделения" as b on a."Отдел"=b."RecordId"
join "SAOOG"."Организации" as c on c."RecordId"=b."Root"
where a."Фиктивный"='0' 
and a."Name"!='SYSTEM1' 
and a."Name"!='System' 
and a."Name"!='Admin'
and a."RecordId" !='00000000-0000-0000-0000-000000000000'
group by c."Name"
'''
                sql_query2 = '''
select sum(count) from (select c."Name", count(*)
from "SAOOG"."SystemUsers" as a
join "SAOOG"."Подразделения" as b on a."Отдел"=b."RecordId"
join "SAOOG"."Организации" as c on c."RecordId"=b."Root"
where a."Фиктивный"='0' 
and a."Name"!='SYSTEM1' 
and a."Name"!='System' 
and a."Name"!='Admin'
and a."RecordId" !='00000000-0000-0000-0000-000000000000'
group by c."Name") as x
'''
                sql_query3 = '''
select count(*) from "SAOOG"."Организации" where "Домен" !='00000000-0000-0000-0000-000000000000'
'''
                sql_query4 = '''
select b."Name" from "SAOOG"."SystemDomains" as a
join "SAOOG"."Организации" as b on a."RecordId"=b."Домен"
where a."Owner"='1'
order by b."Name" asc
'''
                sql_query5 = '''
select count(*) from "SAOOG"."SystemDomains" where "Owner"='1'
'''

                if '1' in list:
                    try:
                        cursor.execute(sql_query1)
                        for row in cursor:
                            print("ОВУ = ", row[0], )
                            print("Лицензий в ОВУ = ", row[1])
                    # results = cursor.fetchall()[:2]
                    # print(results)
                    except(Exception, Error) as error:
                        print("Ошибка при выполнении запроса: ", error)
                elif '2' in list:
                    try:
                        cursor.execute(sql_query2)
                        for row in cursor:
                            print("Общее колличество лицензий = ", row[0])
                    except(Exception, Error) as error:
                        print("Ошибка при выполнении запроса: ", error)
                elif '3' in list:
                    try:
                        cursor.execute(sql_query3)
                        for row in cursor:
                            print("Общее колличество организаций = ", row[0])
                    except(Exception, Error) as error:
                        print("Ошибка при выполнении запроса: ", error)
                elif '4' in list:
                    try:
                        cursor.execute(sql_query4)
                        for row in cursor:
                            print("Имя домена = ", row[0])
                    except(Exception, Error) as error:
                        print("Ошибка при выполнении запроса: ", error)
                elif '5' in list:
                    try:
                        cursor.execute(sql_query5)
                        for row in cursor:
                            print("Общее колличество доменов на объекте = ", row[0])
                    except(Exception, Error) as error:
                        print("Ошибка при выполнении запроса: ", error)
                else:
                    try:
                        for name in list:
                            cursor.execute(sql_query, ("{%"+name+"%}",))
                            for row in cursor:
                                print("ОВУ = ", row[0], )
                                print("Лицензий в ОВУ = ", row[1])
                    except(Exception, Error) as error:
                        print("Ошибка при выполнении запроса: ", error)
    elif answer == 2:
        while True:
            print('''
\t1 - Размер определенной БД
\t2 - Размер всех БД
\t0 - Назад
            ''')
            answer = str(input("Что делаем?  ")).lower()
            list = answer.split()
            if '0' in list:
                print("Идем назад")
                break
            elif '1' in list:
                try:
                    name_bd = str(input("Введите название БД:  ")).lower()
                    list = name_bd.split()
                    sql_query = '''select pg_database.datname, 
                    pg_size_pretty(pg_database_size(pg_database.datname))
                    from pg_database where datname ilike any(%s)
                    '''
                    for name in list:
                        cursor.execute(sql_query, ("{%"+name+"%}",))
                        for row in cursor:
                            print("Наименование БД = ", row[0], )
                            print("Размер БД = ", row[1])
                except(Exception, Error) as error:
                    print("Ошибка при выполнении запроса: ", error)
            elif '2' in list:
                try:
                    sql_query = '''select pg_database.datname, 
                    pg_size_pretty(pg_database_size(pg_database.datname))
                    from pg_database where datname != 'template0' 
                    and  datname != 'template1'
                    '''
                    cursor.execute(sql_query)
                    for row in cursor:
                        print("Наименование БД = ", row[0], )
                        print("Размер БД = ", row[1])
                except(Exception, Error) as error:
                    print("Ошибка при выполнении запроса: ", error)