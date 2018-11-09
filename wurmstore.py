import sqlite3

class WurmStore:

    def __init__(self, dblocation = 'database'):
        self._connection = sqlite3.connect(dblocation)
    
    def _cursor(self):
        return self._connection.cursor()

    def foo(self):
        pass