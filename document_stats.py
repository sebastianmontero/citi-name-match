import pathlib
import random
from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim.similarities import Similarity
from db_manager import DBManager
from data_cleaner import DataCleaner
from document_processor import DocumentProcessor
from dao.externos_dao import ExternosDao
from dao.internos_dao import InternosDao
from dao.possible_matches_doc_dao import PossibleMatchesDocDao


class DocumentStats(object):

    def __init__(self, documents, id):
        self._processor = DocumentProcessor()
        self._documents = documents
        self._id = id
        self._processed_docs = None
        self._dictionary = None

    def _get_processed_docs(self):
        if not self._processed_docs:
            print('Processing documents...')
            self._processed_docs = self._processor.process_documents(
                self._documents)
            self._processed_docs = [[
                word for word_list in self._processed_docs for word in word_list]]
        return self._processed_docs

    def _get_dictionary(self):
        if not self._dictionary:
            path = self._get_dictionary_path()
            if path.exists():
                print('Dictionary already exists, loading from file...')
                self._dictionary = Dictionary.load(str(path))
            else:
                print('Dictionary does not exist, creating...')
                self._dictionary = Dictionary(self._get_processed_docs())
                self._dictionary.save(str(path))
        return self._dictionary

    def _doc2bow(self, doc):
        return self._get_dictionary().doc2bow(doc)

    def _id2word(self, id):
        return self._get_dictionary().get(id)

    def _get_data_path(self, ext):
        return pathlib.Path(__file__).parent / 'data/{}.{}'.format(self._id, ext)

    def _get_dictionary_path(self):
        return self._get_data_path('dict')

    def top_common_words(self, limit=1000):
        processed_docs = self._get_processed_docs()
        bow = self._doc2bow(processed_docs[0])
        bow.sort(key=lambda b: b[1], reverse=True)
        for index, b in enumerate(bow):
            if index >= limit:
                return
            print('{} : {}'.format(self._id2word(b[0]), b[1]))


class DocumentStatsExercise(object):

    def __init__(self):
        self._cleaner = DataCleaner()

    def stats(self):
        with DBManager.get_engine().connect() as conn:
            """ externals = ExternosDao.select(conn)
            external_sn = self._cleaner.clean_external_clients(externals)
            external_names = self._cleaner.extract_names(external_sn) """
            internals = InternosDao.select(conn)
            internal_sn = self._cleaner.clean_internal_clients(internals)
            internal_names = self._cleaner.extract_names(internal_sn)
            print(internal_names)
            doc_stats = DocumentStats(internal_names, 'internal_names_stats')
            """ doc_stats.top_common_words(10000) """


exercise = DocumentStatsExercise()
exercise.stats()
