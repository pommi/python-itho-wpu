import sqlite3
from sqlite3 import Error


class sqlite:
    def __init__(self, db_file):
        self.conn = self.connect(db_file)

    def connect(self, db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e)
        return conn

    def execute(self, query, params=()):
        try:
            self.conn.row_factory = sqlite3.Row
            c = self.conn.cursor()
            c.execute(query, params)
            return [dict(row) for row in c.fetchall()]
        except Error as e:
            print("sqlite_execute failed for: {}, {}".format(query, params))
            print("Error:", e)

    def executemany(self, query, data):
        try:
            c = self.conn.cursor()
            c.executemany(query, data)
        except Error as e:
            print("sqlite_executemany failed for: {}, {}".format(query, data))
            print("Error:", e)

    def create_table(self, t):
        if t.startswith("datalabel"):
            query = """CREATE TABLE {} (
                id real,
                name text,
                title text,
                tooltip text,
                unit text
            );""".format(
                t
            )
        elif t.startswith("counters"):
            query = """
            CREATE TABLE {} (
                id real,
                name text,
                title text,
                tooltip text,
                unit text
            );""".format(
                t
            )
        elif t.startswith("handbed"):
            query = """
            CREATE TABLE {} (
                id real,
                name text,
                name_factory text,
                min real,
                max real,
                def real,
                title text,
                tooltip text,
                unit text
            );""".format(
                t
            )
        elif t.startswith("parameterlijst"):
            query = """
            CREATE TABLE {} (
                id real,
                name text,
                name_factory text,
                min real,
                max real,
                def real,
                title text,
                description text,
                unit text
            );""".format(
                t
            )
        elif t.startswith("versiebeheer"):
            query = """
            CREATE TABLE {} (
                version integer primary key,
                datalabel integer,
                parameterlist integer,
                handbed integer,
                counters interger
            );""".format(
                t
            )
        self.execute(query)
        self.conn.commit()

    def insert(self, t, data):
        if t.startswith("datalabel"):
            query = """
            INSERT INTO {} (id, name, title, tooltip, unit)
            VALUES (?, ?, ?, ?, ?);
            """.format(
                t
            )
        elif t.startswith("parameterlijst"):
            query = """
            INSERT INTO {} (id, name, name_factory, min, max, def, title, description, unit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """.format(
                t
            )
        elif t.startswith("counters"):
            query = """
            INSERT INTO {} (id, name, title, tooltip, unit)
            VALUES (?, ?, ?, ?, ?);
            """.format(
                t
            )
        elif t.startswith("handbed"):
            query = """
            INSERT INTO {} (id, name, name_factory, min, max, def, title, tooltip, unit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """.format(
                t
            )
        elif t.startswith("versiebeheer"):
            query = """
            INSERT INTO {} (version, datalabel, parameterlist, handbed, counters)
            VALUES (?, ?, ?, ?, ?);
            """.format(
                t
            )
        self.executemany(query, data)
        self.conn.commit()
