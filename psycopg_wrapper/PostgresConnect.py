from psycopg2 import pool
from psycopg2.extras import DictCursor, NamedTupleCursor

from psycopg_wrapper.validator import is_crud


class PostgresConnect(object):
    def __init__(self, database: str, username: str, password: str, schema: str = "public", host: str = "127.0.0.1",
                 port: int = 5432, min_connection: int = 1, max_connection: int = 20, type_cursor: str = ''):
        self.database = database
        self.user = username
        self.password = password
        self.schema = schema
        self.host = host
        self.port = port
        self.min_connection = min_connection
        self.max_connection = max_connection
        if type_cursor:
            self._cursor_factory = DictCursor if type_cursor == 'dict' else NamedTupleCursor
        else:
            self._cursor_factory = None
        self._create_connection_pool()

    def _create_connection_pool(self):
        self.connection_pool = pool.ThreadedConnectionPool(
            self.min_connection,
            self.max_connection,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            options=f"-c search_path={self.schema}",
        )

    def _get_connection(self):
        try:
            connection = self.connection_pool.getconn()
            if connection.closed != 0:
                return self._get_connection()
            return connection
        except pool.PoolError:
            return self._get_connection()
        except:
            return self._get_connection()

    def _put_connection(self, con):
        self.connection_pool.putconn(con)

    def _create_cursor(self, connection):
        if connection.closed != 0:
            connection = self._get_connection()
        if self._cursor_factory:
            return connection.cursor(cursor_factory=self._cursor_factory)
        else:
            return connection.cursor()

    @staticmethod
    def _close_cursor(cursor):
        cursor.close()

    @staticmethod
    def _where(where=None):
        if where and len(where) > 0:
            return ' WHERE %s' % where[0]
        return ''

    @staticmethod
    def _order(order=None):
        sql = ''
        if order:
            sql += ' ORDER BY %s' % order[0]

            if len(order) > 1:
                sql += ' %s' % order[1]
        return sql

    @staticmethod
    def _limit(limit):
        if limit:
            return ' LIMIT %d' % limit
        return ''

    @staticmethod
    def _offset(offset):
        if offset:
            return ' OFFSET %d' % offset
        return ''

    @staticmethod
    def _returning(returning):
        if returning:
            return ' RETURNING %s' % returning
        return ''

    @staticmethod
    def _format_insert(data):
        cols = ",".join(data.keys())
        values = ",".join(["%s" for _ in data])

        return cols, values

    @staticmethod
    def _format_update(data):
        return "=%s,".join(data.keys()) + "=%s"

    def _execute_with_autocommit(self, connection, query, params):
        cursor = self._create_cursor(connection)
        cursor.execute(query, params)
        if cursor.description:
            all_data = cursor.fetchall()
            self._close_cursor_connection(cursor, connection)
            return all_data
        self._close_cursor_connection(cursor, connection)
        return True

    def _execute_without_autocommit(self, connection, query, params):
        cursor = self._create_cursor(connection)
        cursor.execute(query, params)
        connection.commit()
        if cursor.description:
            all_data = cursor.fetchall()
            self._close_cursor_connection(cursor, connection)
            return all_data
        self._close_cursor_connection(cursor, connection)
        return True

    def execute_query(self, query, params=None):
        connection = self._get_connection()
        if not is_crud(query):
            connection.autocommit = True
            return self._execute_with_autocommit(connection, query, params)
        return self._execute_without_autocommit(connection, query, params)

    def truncate(self, table, restart_identity=False, cascade=False):
        """Truncate a table or set of tables"""
        sql = 'TRUNCATE %s'
        if restart_identity:
            sql += ' RESTART IDENTITY'
        if cascade:
            sql += ' CASCADE'
        self.execute_query(sql % table)

    def drop(self, table, cascade=False):
        """Drop a table"""
        sql = 'DROP TABLE IF EXISTS %s'
        if cascade:
            sql += ' CASCADE'
        self.execute_query(sql % table)

    def create(self, table, schema):
        """Create a table with the schema provided"""
        self.execute_query('CREATE TABLE %s (%s)' % (table, schema))

    def insert(self, table, data, returning=None):
        """Insert a record"""
        cols, values = self._format_insert(data)
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table, cols, values)
        sql += self._returning(returning)
        cur = self.execute_query(sql, list(data.values()))
        return cur.fetchone() if returning else cur.rowcount

    def select(self, table=None, fields=(), where=None, order=None, limit=None, offset=None):
        """Select from table"""
        sql = 'SELECT %s FROM %s' % (", ".join(fields), table) \
              + self._where(where) \
              + self._order(order) \
              + self._limit(limit) \
              + self._offset(offset)

        return self.execute_query(sql)

    def update(self, table, data, where=None, returning=None):
        """Insert a record"""
        query = self._format_update(data)

        sql = 'UPDATE %s SET %s' % (table, query)
        sql += self._where(where) + self._returning(returning)
        cur = self.execute_query(
            sql, list(data.values()) + where[1] if where and len(where) > 1 else list(data.values())
        )
        return cur.fetchall() if returning else cur.rowcount

    def delete(self, table, where=None, returning=None):
        """Delete rows based on a where condition"""
        sql = 'DELETE FROM %s' % table
        sql += self._where(where) + self._returning(returning)
        cur = self.execute_query(sql, where[1] if where and len(where) > 1 else None)
        return cur.fetchall() if returning else cur.rowcount

    def _close_cursor_connection(self, cursor, connection):
        self._close_cursor(cursor)
        self._put_connection(connection)
