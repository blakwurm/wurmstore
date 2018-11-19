from wurmstore.facts import *

class WurmStoreBase:
    """
    Base class for the WurmStore API.

    Depends on the child class implimenting the following methods (see sqlite_store for a reference):

    _insert_facts(self, facts)
        Accepts an iterator of facts, and adds them to the store. Must accept both facts.Fact and a seven-value list/tuple
    
    _insert_transactions_(transactions)
        Accepts an iterator of transactions, and adds them to the store. Must accept both facts.Transaction and a two-value list/tuple
    
    _insert_insertion_(insertion)
        Accepts a facts.Insertion and adds the contained facts and transactions to the store
    
    _read_(search_query)
        Accepts a search query and returns a ReadResult
    
    _get_raw_facts(timestamp_begin, timestamp_end)
        Accepts a timestamp range, and returns the raw facts as a generator of plain lists/tuples
    
    _get_raw_transactions(timestamp_begin, timestamp_end)
        Accepts a timestamp range, and returns the raw transactions as a generator of plain lists/tuples
    """

    def __init__(self, store_type, store_location):
        self.store_type = store_type
        self.store_location = store_location

    def Transaction(self):
        return Transaction(transaction_id = genID('transaction'), timestamp = get_now_in_millis())

    def Fact(self, *, name, body, entity_id, fact_type="TEXT", fact_operation = 'ADD', transaction_id="", timestamp=0):
        return Fact(name=name, body=body, entity_id=entity_id, fact_type=fact_type, fact_operation=fact_operation, transaction_id=transaction_id)
    
    def insert(self, entity):
        try:
            transaction = self.Transaction()
            #self.__insert_head__(transaction)
            entity_facts = self._prepare_for_insertion_(entity, transaction)
            insert = Insertion(transaction = transaction, facts = entity_facts, successful = True)
            return self._insert_insertion_(insert)
        except Exception as e:
            #return Insertion(transaction = None, facts = None, successful = False)
            return InsertionResult(results = [], error = e)

    def read(self, search_query):
        if (not query_well_formed(search_query)):
            return ReadResult(results = [], error = TypeError('search_query must be formed correctly'))
        try:
            return self._read_(search_query)
        except Exception as e:
            return ReadResult(results = [], error = e)
        
    def get_raw_data(self):
        facts = self._get_raw_facts_()
        transactions = self._get_raw_transactions_()
        return {'facts': facts, 'transactions': transactions}
    
    def restore_from_raw(self, raw_data):
        self._insert_facts_(raw_data['facts'])
        self._insert_transactions_(raw_data['transactions'])
    
    def dict_to_facts(self, entity, transaction = None):
        return dict_to_facts(entity, transaction if transaction else self.Transaction())
    
    def facts_to_dicts(self, facts):
        dicts = dict()  
        for fact in facts:
            old_entity = dicts.get(fact.entity_id, {'id': fact.entity_id})
            new_entity = {**old_entity, **{fact.name: convert_fact_body(fact)}}
            dicts.update({fact.entity_id: new_entity})
        return dicts

    def _prepare_for_insertion_(self, entity, transaction):
        if isinstance(entity, dict):
            return dict_to_facts(entity=entity, transaction=transaction)
        elif isinstance(entity, list):
            return [Fact._make(x)._replace(transaction_id = transaction.transaction_id, timestamp = transaction.timestamp) for x in entity]
        else:
            raise TypeError('entity should be a dict, or a list of lists/tuples')
    
