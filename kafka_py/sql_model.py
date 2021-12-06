import sqlite3 as sqlite

class SQLModel(object):
    def __init__(self, Mode=1):
        # Mode : 0: create; 1:query restart; 2: query;
        super().__init__()
        self.db_connect = sqlite.connect('twitter_message.db')
        self.cur = self.db_connect.cursor()
        if Mode==0:
            self.cur.execute("CREATE TABLE messages(id INT PRIMARY KEY, text TEXT)")
        elif Mode==1: 
            self.cur.execute("DROP TABLE messages")
            self.cur.execute("CREATE TABLE messages(id INT PRIMARY KEY, text TEXT)")
        self.max_id = self.get_maxId()
        
    def get_maxId(self,):
        max_id = self.cur.execute('select max(id) from messages').fetchone()[0]
        if max_id is None:
            max_id = 1
        return max_id

    def insert(self, data):
        execute_success = False
        final_id = self.max_id
        while not execute_success:
            try:
                sql_sen = "insert into messages values({}, '{}')"\
                        .format(final_id, data['value'])
                # print(sql_sen)
                self.cur.execute(sql_sen)
                final_id=True
            except:
                # failed, get a new Id
                self.max_id = self.get_maxId()

        self.db_connect.commit()
        return final_id

    def query(self, query_ids):
        """
            Given a list of query_id of messages, 
            return the text of these messages.
        """
        text_list = []
        for query_id in query_ids:
            sql_sen = "select text from messages where id = {}".format(query_id)
            print(sql_sen)
            text = self.cur.execute(sql_sen).fetchone()[0]
            text_list.append(text)

        return text_list

    def make_data(self, id, value):
        data = {}
        data['id'] = id
        data['value'] = value
        return data

    def simple_insert(self):
        data = self.make_data(100, 'dfdfdf')
        self.insert(data)
    
    def simple_query(self):
        self.query([100])

    # from sql_model import SQLModel