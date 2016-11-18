import time
import os
import gevent
import gevent.event
import redis
import json
import sqlite3

from json import JSONEncoder

_message_listener = None
_message_listener_init_event = gevent.event.Event()
_message_event = gevent.event.AsyncResult()
_all_messages = {}


class MessageEncoder(JSONEncoder):
    def default(self, o):
        return o.__repr__()


def from_json(msg_json):
    return Message(**msg_json)


def get_redis_connection():
    pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    conn = redis.Redis(connection_pool=pool)
    return conn


def create_tables():
    conn = sqlite3.connect('messaging')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages
            (message_id int,
            message varchar(255),
            account_ids varchar(255),
            created_time time)''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS read_messages
            (account_id int,
            message_id int)''')

    conn.commit()


class Message(object):

    def __init__(self, message, account_ids=None,
                 message_id=None, created_time=None):
        self.message_id = message_id or os.urandom(4).encode('hex')
        self.message = message
        # account_ids can be None (all) or a list
        # of account_ids separated by comma
        self.account_ids = account_ids
        self.account_ids_list = self.account_ids.split(',') if self.account_ids else []
        self.created_time = created_time or time.time()

    def is_valid(self, account_id):
        """Check for validity for the user"""
        return account_id in self.account_ids_list

    def save_in_db(self):
        conn = sqlite3.connect('messaging')
        cursor = conn.cursor()
        query = '''INSERT INTO messages (message_id, message, account_ids, created_time)
        VALUES ("{}", "{}", "{}", "{}")'''.format(self.message_id, self.message,
                                                  self.account_ids, self.created_time)
        cursor.execute(query)

        conn.commit()

    def publish_in_redis(self):
        store = get_redis_connection()
        store.publish('messages', json.dumps(self, cls=MessageEncoder))

    def __repr__(self):
        """Return the string representation of the object."""
        return str(self.json_equivalent())

    def json_equivalent(self):
        return {
            'message_id': self.message_id,
            'message': self.message,
            'account_ids': self.account_ids,
            'created_time': self.created_time
        }


def create_message(message, account_ids=None):
    # Loads all messages into in-memory
    print 'Creating new message for User(s): {}'.format(account_ids)
    load_all_messages_from_database()

    mssg = Message(message, account_ids)
    mssg.save_in_db()
    mssg.publish_in_redis()

    return mssg.message_id


def check_message(account_id, last_checked=None):
    print 'Checking for messages for User: {}'.format(account_id)
    if not last_checked:
        # FIXME: Use this to check for latest messages
        last_checked = 0

    load_all_messages_from_database()

    # In memory check
    messages = _get_messages(account_id)
    if messages:
        return {'messages': messages}

    # long polling
    messages = _wait_for_messages(account_id)
    return {'messages': messages}


def _get_messages(account_id):
    """Return list of unread messages from global list of messages."""
    print 'Checking in-memory..'
    global _all_messages
    messages = []

    for message in _all_messages.values():
        if message.is_valid(account_id):
            # check for validity with user
            messages.append(message)

    if messages:
        read_message_ids = get_read_messages(account_id)
        unread_messages_list = [msg for msg in messages if msg.message_id not in read_message_ids]
        messages = unread_messages_list

    return messages


def _wait_for_messages(account_id):
    """Return list of unread messages after waiting for new messages."""
    print 'Starting a long poll for messages..'
    messages = long_polling(account_id)

    if messages:
        read_message_ids = get_read_messages(account_id)
        unread_messages_list = [msg for msg in messages if msg.message_id not in read_message_ids]
        messages = unread_messages_list

    return messages


def load_all_messages_from_database():
    """Check for messages in the database.
    If messages exist, populate the redis cache.
    """
    global _all_messages
    if _all_messages:
        return

    print 'Loading messages from DB'
    conn = sqlite3.connect('messaging')
    cursor = conn.cursor()

    cursor.execute('''
SELECT message_id,
       message,
       account_ids,
       created_time
  FROM messages''')

    messages = {}
    for row in cursor.fetchall():
        params = {key: row[index] for key, index in
                   zip(['message_id', 'message', 'account_ids',
                        'created_time'],
                       range(4))}

        message = Message(**params)
        messages[message.message_id] = message

    _all_messages = messages

    # Start listening on Redis for new messages so we don't have to
    # load again from DB
    _initialize_listening()


def long_polling(account_id):
    """Wait for messages from the redis subscription channel."""
    # Check latest messages from the global message list & return
    print 'Beginning to wait'
    polling_time = 5
    start_time = time.time()
    global _message_event

    while True:
        wait_time = polling_time - (time.time() - start_time)
        print 'wait time {}'.format(wait_time)
        try:
            message = _message_event.get(timeout=wait_time)
        except gevent.timeout.Timeout:
            break

        if message.is_valid(account_id):
            return [message]

    print 'long poll ended with no new messages'
    return []


def _initialize_listening():
    """Initialize the process listening from Redis"""
    global _message_listener
    if _message_listener:
        return

    _message_listener = gevent.spawn(listen_for_messages)
    # Wait until the thread has done basic initialization
    # global _message_listener_init_event
    # _message_listener_init_event.wait()
    # time.sleep(10)


def listen_for_messages():
    """Listen for message on Redis pubsub channel"""
    global _message_listener_init_event
    try:
        print 'Initiating pub/sub channel'
        store = get_redis_connection()
        client = store.pubsub()
        client.subscribe('messages')
        print 'Done with setup'
        _message_listener_init_event.set()
        # Now we are ready to listen forever
        _listen_for_messages(client)
    except Exception as e:
        # Make sure reinitialization of the cache happens so that we
        # didn't miss any events during error
        global _all_messages
        global _message_listener
        _all_messages = None
        _message_listener = None
        _message_listener_init_event = gevent.event.Event()


def _listen_for_messages(client):
    """Appending subscribed messages to global message list."""
    # General syntax of subscribed message:
    # {'pattern': None,
    #  'type': 'message',
    #  'channel': 'messages',
    #  'data': "<data>"}
    print 'Listening forever..'
    for full_message in client.listen():
        print 'Got new message: {}'.format(full_message)
        if full_message['data'] == 1L:
            continue

        params = json.loads(full_message['data'], object_hook=from_json)
        message = Message(**params)

        # update in memory
        _all_messages[message.message_id] = message

        # awaken listeners
        global _message_event
        _message_event.set(message)
        # reset event
        _message_event = gevent.event.AsyncResult()


def mark_as_read(account_id, message_id):
    """User calls this to ignore a message from coming again"""
    # Store in DB
    conn = sqlite3.connect('messaging')
    cursor = conn.cursor()
    query = '''INSERT INTO read_messages (account_id, message_id) VALUES ("{}", "{}")'''.format(account_id, message_id)
    cursor.execute(query)
    conn.commit()

    # Store in cache
    store = get_redis_connection()
    key = 'read_messages:' + str(account_id)
    store.lpush(key, message_id)


def get_read_messages(account_id):
    """Gives list of read message IDS"""
    read_message_ids = []
    store = get_redis_connection()
    key = 'read_messages:' + str(account_id)

    # Read from memory store if available otherwise database
    if store.exists(key):
        read_message_ids = store.lrange(key, 0, -1)
    else:
        conn = sqlite3.connect('messaging')
        cursor = conn.cursor()
        query = ''' SELECT message_id FROM read_messages WHERE account_id = "{}" '''.format(account_id)
        cursor.execute(query)

        for row in cursor.fetchall():
            read_message_ids.append(row[0])

        # Add to memory store from DB
        if read_message_ids:
            store.lpush(key, *read_message_ids)

    return set(read_message_ids)


if __name__ == '__main__':
    # Creating tables if necessary
    create_tables()

    # Clearing all data
    global _all_messages
    _all_messages = {}
    conn = sqlite3.connect('messaging')
    cursor = conn.cursor()
    cursor.execute('''DELETE FROM messages ''')
    conn.commit()

    # Start testing
    mssg_id = create_message('check', '123')
    print check_message('123')
    mark_as_read('123', mssg_id)
    print check_message('123')

    # print 'gng to wait'
    # gevent.sleep(2)
    # print 'Creating message now..'
    # cr = gevent.spawn(create_message, 'Hi', '123')
    # gevent.joinall([cr])
    # print 'Ends here!!!'

    # create_message('Hi', '123, 456')
    # create_message('Hi')

    """
    Testing procedure:
    create_message
    check_message
    mark_as_read

    increase long polling to maybe 15secs
    spawn two threads in the following way:
    spawn(check_message)
    time.sleep(5)
    spawn(create_message)
    <check message should exit and return message>
    """
