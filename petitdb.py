#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File: 'petitdb.py'
"""Simple interface to store/retreive objects from shelves."""

import os
import copy
import shelve


__version__ = '1.2'


########################
# Exceptions
########################

class DBError(Exception):
    """Base error"""
    pass


class DBInitializationError(DBError):
    """raised when the creation of a db instance fails."""
    pass


class TableReferenceError(DBError):
    """raised when trying to manipulate an unexisting talbe."""
    pass


class DBKeyError(DBError):
    """raised when trying to manipulate an unexisting key."""
    pass


class DuplicateKeyError(DBKeyError):
    """raised when trying to insert an unexisting key."""
    pass


class ReadOnlyDatabaseError(DBError):
    """raised when trying to manipulate data on a read-only database."""
    pass


########################
# SmallDB Class
########################

class SmallDB:
    """Interface to store and retreive data or objects from shelve db.

    A SmallDB object is basically a shelve that can store many dictionnaries.
    To provide an easy to use interface, the SmallDB object presents the dicts
    it holds as tables, and each table can store records that consist of a key
    and a value. 
    Data manipulation on `SmallDB` objects can be done through the following
    methods:
     insert:        insert records
     update:        replace the value of an existing records
     add:           convinient method to update records
     append:        convinient method to update records
     remove:        remove records
     create_table:  create tables(dicts)
     remove_table:  remove tables(dicts)

    The following methods are also provided to retreive data:
     select:        retreive records
     get_tables:    retreive all tables from the object
     get_keys:      retreive all keys from tables

    a `SmallDB` object can be instantiated in two modes:
     - ro: read-only; all methods that try to modify data are not allowed
           Calling on a method that modifies data will raise a
           ReadOnlyDatabaseError exception
     - rw: read-write; all methods are supported.
     ** The default mode is 'rw'
     Examples:
       - create a `SmallDB` object in ro mode: db = SmallDB(filename, 'ro')
       - create a `SmallDB` object in rw mode: db = SmallDB(filename, 'rw')
    """

    def __init__(self, dbf=None, mode='rw'):
        """Constructor: Creates a new `SmallDB` object

        @param dbf: a filename where data will be saved.
        @param mode: string representing the mode
                    'ro':   read-only  --> no data can be added
                    'rw':   read-write --> data manipilation is allowed
                    Default mode is 'rw'
        """
        if mode != 'ro' and mode != 'rw':
            raise DBInitializationError("Wrong mode!!!")
        else:
            self.mode = mode
        self.dbfile = dbf
        self.__open__()

    def __open__(self):
        if self.dbfile is not None:
            try:
                self.db = shelve.open(self.dbfile)
                self.db_ref = dict()
                for key in self.db:
                    self.db_ref[key] = self.db[key]
            except Exception as e:
                raise DBInitializationError("Could not open db file - {0:s}: {1:s}".format(self.dbfile, e))
        else:
            raise DBInitializationError('DB filename missing.')
        self.log = dict()
        if self.mode == 'ro':
            self.db.close()

    def create_table(self, table_name):
        """create a table in db.

        @param table_name: name to assign to the table
        """
        if self.mode == 'ro':
            raise ReadOnlyDatabaseError("Can't modify a read-only DB!!!")
        self.db_ref[table_name] = dict()
        self.log[table_name] = ''

    def select(self, table_name, key=None):
        """return the object associated with key from a table.

        @param table_name: table
        @param key:        key to search the object
        """
        if key is not None:
            try:
                table = self.db_ref[table_name]
            except KeyError:
                raise TableReferenceError("%s is not in DB." % table_name)
            try:
                return table[key]
            except KeyError:
                raise DBKeyError("%s not found in %s" % (key, table_name))
        else:
            try:
                table = self.db_ref[table_name]
            except KeyError:
                raise TableReferenceError("%s is not in DB." % table_name)
            data = list()
            for key in table:
                data.append(table[key])
            return data

    def insert(self, table_name, key, obj):
        """insert an object in a table.

        @param table_name: table
        @param key:        key
        @param obj:        the object to insert
        """
        if self.mode == 'ro':
            raise ReadOnlyDatabaseError("Can't modify a read-only DB!!!")
        try:
            table = self.db_ref[table_name]
        except KeyError:
            self.create_table(table_name)
            self.insert(table_name, key, obj)
            return
        try:
            data = table[key]
            del data
        except KeyError:
            table[key] = obj
            self.db_ref[table_name] = table
        else:
            raise DuplicateKeyError("key [%s] exists in table [%s]" % (
                key, table_name))
        self.log[table_name] = ''

    def keys(self, table_name):
        """return all keys associated with a table

        @param table_name: table
        """
        try:
            return self.db_ref[table_name].keys()
        except KeyError:
            raise TableReferenceError("%s is not in DB." % table_name)

    def tables(self):
        """return all tables

        Keyword arguments:
        """
        return self.db_ref.keys()

    def update(self, table_name, key, obj):
        """update an object in a table.

        @param table_name: table
        @param key:        key
        @param obj:        the object to update
        """
        if self.mode == 'ro':
            raise ReadOnlyDatabaseError("Can't modify a read-only DB!!!")
        try:
            table = self.db_ref[table_name]
        except KeyError:
            raise TableReferenceError("%s is not in DB." % table_name)
        try:
            table[key] = obj
            self.db_ref[table_name] = table
        except KeyError:
            raise DBKeyError("%s not found in %s" % (key, table_name))
        self.log[table_name] = ''

    def append(self, table_name, key, obj):
        """append an object to an existing object in a talbe.
        the object we append to must have a append() method, otherwise an
        exception will be raised.

        @param table_name: table
        @param key:        key
        @param obj:        the object to append to the selected object
        """
        if self.mode == 'ro':
            raise ReadOnlyDatabaseError("Can't modify a read-only DB!!!")
        try:
            tmp_obj = self.select(table_name, key)
            tmp_obj.append(obj)
            self.update(table_name, key, tmp_obj)
        except DBKeyError:
            raise DBKeyError("%s not found in %s" % (key, table_name))
        except TableReferenceError:
            raise TableReferenceError("%s is not in DB." % table_name)
        except AttributeError, e:
            print e
        self.log[table_name] = ''

    def add(self, table_name, key, obj):
        """add an object to un existing object in a talbe.
        the object we add to must support the + operator, otherwise an
        exception will be raised.

        @param table_name: table
        @param key:        key
        @param obj:        the object to add to the selected object
        """
        if self.mode == 'ro':
            raise ReadOnlyDatabaseError("Can't modify a read-only DB!!!")
        try:
            tmp_obj = self.select(table_name, key)
            tmp_obj += obj
            self.update(table_name, key, tmp_obj)
        except DBKeyError:
            raise DBKeyError("%s not found in %s" % (key, table_name))
        except TableReferenceError:
            raise TableReferenceError("%s is not in DB." % table_name)
        except AttributeError, e:
            print e
        self.log[table_name] = ''

    def remove(self, table_name, key):
        """remove an object from a table.

        @param table_name: table
        @param key:        key
        """
        if self.mode == 'ro':
            raise ReadOnlyDatabaseError("Can't modify a read-only DB!!!")
        try:
            table = self.db_ref[table_name]
        except KeyError:
            raise TableReferenceError("%s is not in DB." % table_name)
        try:
            del table[key]
            self.db_ref[table_name] = table
        except KeyError:
            raise DBKeyError("%s not found in %s" % (key, table_name))
        self.log[table_name] = ''

    def remove_table(self, table_name):
        """remove a table from the db.

        @param table_name: table to remove.
        """
        if self.mode == 'ro':
            raise ReadOnlyDatabaseError("Can't modify a read-only DB!!!")
        try:
            del self.db_ref[table_name]
        except KeyError:
            raise TableReferenceError("%s is not in DB." % table_name)
        self.log[table_name] = ''

    def save(self):
        """save changes made after the last commit."""
        if self.mode == 'ro':
            raise ReadOnlyDatabaseError("Can't commit on a read-only DB!!!")
        for key in self.db:
            if key in self.log and key not in self.db_ref:
                del self.db[key]
                del self.log[key]
        for key in self.log:
            self.db[key] = self.db_ref[key]

    def drop(self):
        """drop changed made after the last commit."""
        if self.mode == 'ro':
            raise ReadOnlyDatabaseError("Nothing to drop on a read-only DB!!!")
        self.__init__(self.dbfile)

    def print_db(self, table_name=None, key=None):
        """print contents of database.

        @param table_name: table to print.
                           *if ommitted, all tables in the db will be printed.
        @param key:        key of data to print.
                           *if ommitted, all data of selected tables will be
                           printed
        """
        if table_name is None:
            for table in self.db_ref:
                try:
                    title = table
                except DBKeyError:
                    title = table
                except TableReferenceError:
                    title = table
                print "*" * (len(title) + 4)
                print "* %s *" % title
                print "*" * (len(title) + 4)
                print "%s\t\t\t\t%-.80s" % ('key', 'data')
                print ("-" * 100)
                if key is None:
                    for k in self.db_ref[table]:
                        print "%s\t\t\t\t%-.80s" % (k, self.select(
                            table, k))
                else:
                    try:
                        print "%s\t\t\t\t%-.80s" % (key, self.select(
                            table, key))
                    except DBKeyError:
                        print "No data was found"
                print "\n"
        else:
            try:
                data = self.db_ref[table_name]
                del data
            except KeyError:
                print "%s not in Database." % table_name
                return
            title = table_name
            print "*" * (len(title) + 4)
            print "* %s *" % title
            print "*" * (len(title) + 4)
            print "%s\t\t\t\t%-.80s" % ('key', 'data')
            print ("-" * 100)
            if key is None:
                for k in self.db_ref[table_name]:
                    print "%s\t\t\t\t%-.80s" % (k, self.select(
                        table_name, k))
            else:
                try:
                    print "%s\t\t\t\t%-.80s" % (key, self.select(
                        table_name, key))
                except DBKeyError:
                    print "No data was found"
            print

    def close(self):
        """save changes after the last commit and close db.

        No data manipulation can be done after calling on close().
        """
        if self.mode == 'ro':
            pass  # Let's allow this at least.
        if len(self.log) != 0:
            self.save()
        self.db.close()


########################
# MemDB Class
########################

class MemDB(SmallDB):
    """Interface to store and retreive objects from in-memory database.

    As a subclass of `SmallDB`, a `MemDB` object has  the same functionnalities
    as a `SmallDB`, except everything is done on memmory. There is no way to
    persistently save date on disk.
    When a filename(dbf) is provided to the constructor, a `MemDB` object will
    replicate the database contained in the provided file, otherwise an empty
    database is returned.
    """

    def __init__(self, dbf=None, mode='rw'):
        """Constructor: Creates a new MemDB object

        @param dbf:  a filename from where to replicate database.
        @param mode: string representing the mode
                     'ro':   read-only  --> no data can be added
                     'rw':   read-write --> data manipilation is allowed
                     Default mode is 'rw'
        """
        SmallDB.__init__(self, dbf, mode)

    def __open__(self):
        if self.dbfile is not None:
            small_db = SmallDB(self.dbfile)
            self.db_ref = copy.deepcopy(small_db.db_ref)
            small_db.close()
            del small_db
        else:
            self.db_ref = dict()
        self.log = dict()

    def replicate_from_smalldb(self, dbf=None):
        """reinitialize object by replicating a new db

        @param dbf:  a filename from where to replicate database.
        """
        if dbf is None and self.dbfile is not None:
            self.__init__(self.dbfile)
        else:
            self.__init__(dbf)

    def replicate_to_smalldb(self, dbf=None):
        """Copy back contents of the `MemDB` to a `SmallDB`

        @param dbf:  a filename to replicate database.
        """
        if dbf is not None:
            smalldb = SmallDB(dbf)
        else:
            smalldb = SmallDB(self.dbfile)

        smalldb.db_ref = copy.deepcopy(self.db_ref)
        smalldb.log = copy.deepcopy(self.log)
        smalldb.save()
        smalldb.close()
        del smalldb

    def save(self):
        """This will do nothing, saving data is not supported"""
        pass

    def close(self):
        """This will do nothing, no shelve is associated"""
        pass
