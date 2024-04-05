import psycopg2
from psycopg2.sql import SQL, Identifier

# 1. Функция, создающая структуру БД (таблицы).

def create_database(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client(
            client_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            surname VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cl_phones(
            phone_id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES client(client_id),
            phone VARCHAR(15) NULL
        );
        """)
        conn.commit()

# 2. Функция, позволяющая добавить нового клиента.

def add_new_client(conn, name_cl, surname_cl, email_cl, phone_cl=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO client(name, surname, email)
        VALUES(%s, %s, %s) RETURNING client_id;
        """, (name_cl, surname_cl, email_cl))
        cl_id = cur.fetchone()[0]
        if phone_cl != None:
            cur.execute("""
            INSERT INTO cl_phones(client_id, phone)
            VALUES(%s, %s)
            """, (cl_id, phone_cl))
        conn.commit()

# 3. Функция, позволяющая добавить телефон для существующего клиента.

def add_phone(conn, client_id, phone_cl):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO cl_phones(client_id, phone)
        VALUES(%s, %s)
        """, (client_id, phone_cl))
        conn.commit()

# 4. Функция, позволяющая изменить данные о клиенте.

def change_client(conn, client_id, name_cl=None, surname_cl=None, email_cl=None, old_phone=None, new_phone=None):
    with conn.cursor() as cur:
        arg_dict = {'name':name_cl, 'surname':surname_cl, 'email':email_cl}
        for key, arg in arg_dict.items():
            if arg:
                cur.execute(SQL("UPDATE client SET {}=%s WHERE client_id=%s").format(Identifier(key)), (arg, client_id))
        if old_phone != None:
            cur.execute("""
            UPDATE cl_phones SET phone=%s
            WHERE client_id=%s AND phone=%s
            """, (new_phone, client_id, old_phone))
            conn.commit()        

# 5. Функция, позволяющая удалить телефон для существующего клиента.

def delete_phone(conn, client_id, phone_cl):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM cl_phones
        WHERE client_id=%s AND phone=%s;
        """, (client_id, phone_cl))
        conn.commit()

# 6. Функция, позволяющая удалить существующего клиента.

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM cl_phones
        WHERE client_id=%s;
        """, (client_id, ))
        cur.execute("""
        DELETE FROM client
        WHERE client_id=%s;
        """, (client_id, ))
        conn.commit()

# 7. Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.

def find_client(conn, name_cl=None, surname_cl=None, email_cl=None, phone_cl=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM client cl
        JOIN cl_phones ph ON cl.client_id = ph.client_id
        WHERE (name=%(name)s OR %(name)s IS NULL)
        AND (surname=%(surname)s OR %(surname)s IS NULL)
        AND (email=%(email)s OR %(email)s IS NULL)
        AND (phone=%(phone)s OR %(phone)s IS NULL);
        """, {"name":name_cl, "surname":surname_cl, "email":email_cl, "phone":phone_cl})
        cl_data = cur.fetchall()
        cl_id = cl_data[0][0]
        nm = cl_data[0][1]
        sr = cl_data[0][2]
        em = cl_data[0][3]
        ph = cl_data[0][6]
        phone_list = []
        if len(cl_data) == 1:
            result = f"client_id: {cl_id}\n name:{nm}\n surname: {sr}\n email: {em}\n phone: {ph}"
        elif len(cl_data) > 1:
            for i, item in enumerate(cl_data):
                phone_list.append(cl_data[i][6])
            result = f"client_id: {cl_id}\n name:{nm}\n surname: {sr}\n email: {em}\n phone: {phone_list}"    
    return result
        
with psycopg2.connect(database="clients_db", user="postgres", password="password") as conn:
    # create_database(conn)
    # add_new_client(conn, 'Ivan', 'Ivanov', 'ivanov@mail.ru', phone_cl='8(945)333-22-11')
    # add_phone(conn, 3, '8(918)222-33-22')
    # change_client(conn, 2, 'Petr', 'Petrov', 'petrov@mail.ru', '8(918)222-33-22', '8(928)100-10-00')
    # delete_phone(conn, 2, '8(928)100-10-00')
    # delete_client(conn, 2)
    # print(find_client(conn, 'Ivan', 'Ivanov', 'ivanov@mail.ru'))

conn.close()