import sqlite3
from contextlib import suppress

class WurmStore:

    def __init__(self, dblocation = 'database'):
        self._connection = sqlite3.connect(dblocation)
    
    def _cursor(self):
        return self._connection.cursor()
    
    def _maketables(self):
        with suppress(Exception):
            c = self._connection
            c.execute("CREATE TABLE IF NOT EXISTS facts ('factID' TEXT PRIMARY KEY, 'factName' TEXT, 'factValue' TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS transactions ('transactionID' TEXT PRIMARY KEY, 'factID' TEXT, 'transactionOperation' TEXT, 'transactionDate' TEXT)")


    def foo(self):
        pass