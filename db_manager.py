import os
import configparser
import threading
from sqlalchemy import create_engine


class DBManager(object):
    _engine = None
    lock = threading.RLock()

    """ @classmethod
    def get_engine(cls):
        if cls._engine is None:
            with cls.lock:
                if cls._engine is None:
                    print('creating engine')
                    config = configparser.ConfigParser()
                    config.read(os.path.join(os.path.dirname(
                        os.path.realpath(__file__)), 'config.ini'))
                    cls._engine = create_engine(config['DB']['connection_url'])
        return cls._engine """

    @classmethod
    def get_engine(cls):
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'config.ini'))
        return create_engine(config['DB']['connection_url'])
