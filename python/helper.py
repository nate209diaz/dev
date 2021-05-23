import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    connnect_db = psycopg2.connect(
    host = "localhost",
    port = "5433",
    database = "test_multi",
    user = "postgres",
    password = "")
    cursor = connnect_db.cursor()
    print("Вы успешно подключились к базе данных test_multi")
except (Exception, Error) as error:
    print("Ошибка на этапе подключения к базе данных test_multi: ", error)
    
exit_code = 0
while exit_code == 0:
    print('''
        1 - Показать колличество лицензий
        0 - Завершить работу и закрыть соединение:  ''')
    answer = int(input("Что делаем?  "))
    if answer == exit_code:
        cursor.close()
        connnect_db.close()
        print('До скорой встречи!')
        break
    elif answer == 1:
        exit_code = 0
        while exit_code == 0:
            print("""
                    Введите название ову например 'урог'
                    если хотите все лицензии напишите 'все'
                    если нужна просто сумма всех лицензий напишите 'сумма'
                    если хотите вернуться назад напишите 'назад': """)
            name_ovu = str(input("Что делаем?  "))
            list = name_ovu.lower()
            if 'назад' in list:
                print("Идем назад")
                break
            else:
                sql_query = f'''select c."Name" as "ОВУ", count(*) as "Сколько лицензий", (select sum("Сколько лицензий") as "Общее колличество лицензий" 
from (select c."Name" as "ОВУ", count(*) as "Сколько лицензий"
from "SAOOG"."SystemUsers" as a
join "SAOOG"."Подразделения" as b on a."Отдел"=b."RecordId"
join "SAOOG"."Организации" as c on c."RecordId"=b."Root"
where a."Фиктивный"='0' and a."Name"!='SYSTEM1' and a."Name"!='System' and a."Name"!='Admin' 
group by c."Name") as x) 
from "SAOOG"."SystemUsers" as a
join "SAOOG"."Подразделения" as b on a."Отдел"=b."RecordId"
join "SAOOG"."Организации" as c on c."RecordId"=b."Root"
where a."Фиктивный"='0' and a."Name"!='SYSTEM1' and a."Name"!='System' and a."Name"!='Admin'  and c."Name" ilike '%%{name_ovu}%%'
group by c."Name"'''
                sql_query2 = f'''select c."Name" as "ОВУ", count(*) as "Сколько лицензий", (select sum("Сколько лицензий") as "Общее колличество лицензий" 
from (select c."Name" as "ОВУ", count(*) as "Сколько лицензий"
from "SAOOG"."SystemUsers" as a
join "SAOOG"."Подразделения" as b on a."Отдел"=b."RecordId"
join "SAOOG"."Организации" as c on c."RecordId"=b."Root"
where a."Фиктивный"='0' and a."Name"!='SYSTEM1' and a."Name"!='System' and a."Name"!='Admin' 
group by c."Name") as x) 
from "SAOOG"."SystemUsers" as a
join "SAOOG"."Подразделения" as b on a."Отдел"=b."RecordId"
join "SAOOG"."Организации" as c on c."RecordId"=b."Root"
where a."Фиктивный"='0' and a."Name"!='SYSTEM1' and a."Name"!='System' and a."Name"!='Admin'
group by c."Name"'''
                if 'все' in list:
                    try:
                        cursor.execute(sql_query2)
                        for row in cursor:
                            print("ОВУ = ", row[0], )
                            print("Лицензий в ОВУ = ", row[1])
                #results = cursor.fetchall()[:2]
                #print(results)
                    except(Exception, Error) as error:
                        print("Ошибка при выполнении запроса: ", error)
                elif 'сумма' in list:
                    try:
                        cursor.execute(sql_query2)
                        results = cursor.fetchmany(1)
                        for row in results:
                            print("Общее колличество лицензий = ", row[2])
                    except(Exception, Error) as error:
                        print("Ошибка при выполнении запроса: ", error)
                else:
                    try:
                        cursor.execute(sql_query, name_ovu)
                        for row in cursor:
                            print("ОВУ = ", row[0], )
                            print("Лицензий в ОВУ = ", row[1])
                #results = cursor.fetchone()[:2]
                #print(results)
                    except(Exception, Error) as error:
                        print("Ошибка при выполнении запроса: ", error)
