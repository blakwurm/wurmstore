from wurmstore.facts import Fact, dict_to_facts, genID

def WurmStore(storage_type = 'memory', location = 'db'):
    """Returns a WurmStore object backed by a given storage type"""
    return storage_types.get(storage_type, 'memory')(location)

def __makeMemoryStore(location = ''):
    from wurmstore.sqlite_store import SQLiteStore
    return SQLiteStore(':memory:')

def __makeSQLiteStore(location):
    from wurmstore.sqlite_store import SQLiteStore
    return SQLiteStore(location)

storage_types = dict()

def add_storage_type(storage_type_name, storage_type_init):
    storage_types.update({storage_type_name: storage_type_init})
    return storage_types

add_storage_type('memory', __makeMemoryStore)
add_storage_type('sqlite', __makeSQLiteStore)
