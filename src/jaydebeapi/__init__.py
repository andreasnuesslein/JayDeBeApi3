# Copyright 2010, 2011, 2012, 2013 Bastian Bowe
# 2015 Andreas Nüßlein
#
# This file is part of JayDeBeApi.
# JayDeBeApi is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# JayDeBeApi is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with JayDeBeApi.  If not, see
# <http://www.gnu.org/licenses/>.

import datetime
import time
import os
from py4j import java_gateway


def _gateway_is_running():
    gwconn = java_gateway.GatewayConnection(gateway_parameters=java_gateway.GatewayParameters(address="127.0.0.1", port=25333))
    res = gwconn.socket.connect_ex((gwconn.address, gwconn.port))
    return True if res == 0 else False


# DB-API 2.0 Module Interface connect constructor
def connect(jclassname, driver_args, jars=None, libs=None):
    """Open a connection to a database using a JDBC driver and return
    a Connection instance.

    jclassname: Full qualified Java class name of the JDBC driver.
    driver_args: Argument or sequence of arguments to be passed to the
           Java DriverManager.getConnection method. Usually the
           database URL. See
           http://docs.oracle.com/javase/6/docs/api/java/sql/DriverManager.html
           for more details
    jars: Jar filename or sequence of filenames for the JDBC driver
    libs: Dll/so filenames or sequence of dlls/sos used as shared
          library by the JDBC driver
    """

    if _gateway_is_running():
        gateway = java_gateway.JavaGateway()
    else:
        driver_args = [driver_args] if isinstance(driver_args, str) else driver_args

        if jars:
            classpath = os.pathsep.join(jars) if isinstance(jars, list) else jars
        else:
            classpath = None

        if libs:
            javaopts = libs if isinstance(libs, list) else [libs]
        else:
            javaopts = []

        gateway = java_gateway.JavaGateway.launch_gateway(
            port=25333, classpath=classpath, javaopts=javaopts, die_on_exit=True)

        java_gateway.java_import(gateway.jvm, 'java.sql.DriverManager')
        gateway.jvm.Class.forName(jclassname)

    connection = gateway.jvm.DriverManager.getConnection(*driver_args)
    if _converters is None:
        types_map = {}
        for type in gateway.jvm.Class.forName("java.sql.Types").getFields():
            types_map[type.getName()] = type.getInt(None)
        _init_converters(types_map)

    return Connection(connection, _converters)


class Connection:
    def __init__(self, jconn, converters):
        self.jconn = jconn
        self._converters = converters

    def close(self):
        self.jconn.close()

    def commit(self):
        self.jconn.commit()

    def rollback(self):
        self.jconn.rollback()

    def cursor(self):
        return Cursor(self, self._converters)


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


class Cursor:
    rowcount = -1
    _meta = None
    _prep = None
    _rs = None
    _description = None

    def __init__(self, connection, converters):
        self._connection = connection
        self._converters = converters

    @property
    def description(self):
        if self._description:
            return self._description
        m = self._meta
        if m:
            count = m.getColumnCount()
            self._description = []
            for col in range(1, count + 1):
                size = m.getColumnDisplaySize(col)
                col_desc = ( m.getColumnName(col),
                             m.getColumnTypeName(col),
                             size,
                             size,
                             m.getPrecision(col),
                             m.getScale(col),
                             m.isNullable(col),
                             )
                self._description.append(col_desc)
            return self._description

        #   optional callproc(self, procname, *parameters) unsupported

    def close(self):
        self._close_last()
        self._connection = None

    def _close_last(self):
        """Close the resultset and reset collected meta data.
        """
        if self._rs:
            self._rs.close()
        self._rs = None
        if self._prep:
            self._prep.close()
        self._prep = None
        self._meta = None
        self._description = None

    # TODO: this is a possible way to close the open result sets
    # but I'm not sure when __del__ will be called
    __del__ = _close_last

    def _set_stmt_parms(self, prep_stmt, parameters):
        for i in range(len(parameters)):
            # print (i, parameters[i], type(parameters[i]))
            prep_stmt.setObject(i + 1, parameters[i])

    def execute(self, operation, parameters=None):
        if not parameters:
            parameters = ()
        self._close_last()
        self._prep = self._connection.jconn.prepareStatement(operation)
        self._set_stmt_parms(self._prep, parameters)
        is_rs = self._prep.execute()
        if is_rs:
            self._rs = self._prep.getResultSet()
            self._meta = self._rs.getMetaData()
            self.rowcount = -1
        else:
            self.rowcount = self._prep.getUpdateCount()
            # self._prep.getWarnings() ???

    def executemany(self, operation, seq_of_parameters):
        self._close_last()
        self._prep = self._connection.jconn.prepareStatement(operation)
        for parameters in seq_of_parameters:
            self._set_stmt_parms(self._prep, parameters)
            self._prep.addBatch()
        update_counts = self._prep.executeBatch()
        # self._prep.getWarnings() ???
        self.rowcount = sum(update_counts)
        self._close_last()

    def fetchone(self):
        #raise if not rs
        if not self._rs.next():
            return None
        row = []
        for col in range(1, self._meta.getColumnCount() + 1):
            sqltype = self._meta.getColumnType(col)
            # print sqltype
            # TODO: Oracle 11 will read a oracle.sql.TIMESTAMP
            # which can't be converted to string easily
            v = self._rs.getObject(col)
            if v:
                converter = self._converters.get(sqltype)
                if converter:
                    v = converter(v)
            row.append(v)
        return tuple(row)

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        # TODO: handle SQLException if not supported by db
        self._rs.setFetchSize(size)
        rows = []
        row = None
        for i in range(size):
            row = self.fetchone()
            if row is None:
                break
            else:
                rows.append(row)
        # reset fetch size
        if row:
            # TODO: handle SQLException if not supported by db
            self._rs.setFetchSize(0)
        return rows

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            else:
                rows.append(row)
        return rows

    # optional nextset() unsupported

    arraysize = 1

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass


apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'

STRING = DBAPITypeObject("CHARACTER", "CHAR", "VARCHAR", "CHARACTER VARYING", "CHAR VARYING", "STRING")
TEXT = DBAPITypeObject("CLOB", "CHARACTER LARGE OBJECT", "CHAR LARGE OBJECT", "XML")
BINARY = DBAPITypeObject("BLOB", "BINARY LARGE OBJECT")
NUMBER = DBAPITypeObject("INTEGER", "INT", "SMALLINT", "BIGINT")
FLOAT = DBAPITypeObject("FLOAT", "REAL", "DOUBLE", "DECFLOAT")
DECIMAL = DBAPITypeObject("DECIMAL", "DEC", "NUMERIC", "NUM")
DATE = DBAPITypeObject("DATE", )
TIME = DBAPITypeObject("TIME", )
DATETIME = DBAPITypeObject("TIMESTAMP", )
ROWID = DBAPITypeObject(())

# DB-API 2.0 Module Interface Exceptions
class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# DB-API 2.0 Type Objects and Constructors

# def _java_sql_blob(data):
#     return jpype.JArray(jpype.JByte, 1)(data)
#
# Binary = _java_sql_blob

def _str_func(func):
    def to_str(*parms):
        return str(func(*parms))

    return to_str


Date = _str_func(datetime.date)
Time = _str_func(datetime.time)
Timestamp = _str_func(datetime.datetime)


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


def _to_datetime(java_val):
    d = datetime.datetime.strptime(str(java_val)[:19], "%Y-%m-%d %H:%M:%S")
    if not isinstance(java_val, str):
        d = d.replace(microsecond=int(str(java_val.getNanos())[:6]))
    return str(d)
    # return str(java_val)


def _to_date(java_val):
    d = datetime.datetime.strptime(str(java_val)[:10], "%Y-%m-%d")
    return d.strftime("%Y-%m-%d")
    # return str(java_val)


def _init_converters(types_map):
    """Prepares the converters for conversion of java types to python
    objects.
    types_map: Mapping of java.sql.Types field name to java.sql.Types
    field constant value"""
    global _converters
    _converters = {}
    for i in _DEFAULT_CONVERTERS:
        const_val = types_map[i]
        _converters[const_val] = _DEFAULT_CONVERTERS[i]

# Mapping from java.sql.Types field to converter method
_converters = None

_DEFAULT_CONVERTERS = {
    'TIMESTAMP': _to_datetime,
    'DATE': _to_date,
    'BINARY': str,
    'DECIMAL': float,
    'NUMERIC': float,
    'DOUBLE': float,
    'FLOAT': float,
    'INTEGER': int,
    'SMALLINT': int,
    'BOOLEAN': bool,
}
