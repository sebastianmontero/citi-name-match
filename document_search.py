
import random
from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim.similarities import Similarity
from db_manager import DBManager
from data_cleaner import DataCleaner
from document_processor import DocumentProcessor
from dao.externos_dao import ExternosDao
from dao.internos_dao import InternosDao


class DocumentSearch(object):

    def __init__(self, documents):
        self._processor = DocumentProcessor()
        self._documents = documents
        self._prepare_for_search()

    def _create_dictionary(self, docs):
        return Dictionary(docs)

    def _create_corpus(self, dictionary, docs):
        return [dictionary.doc2bow(doc) for doc in docs]

    def _create_tfidf_model(self, corpus):
        return TfidfModel(corpus)

    def _create_similarity(self, corpus, tfidf_model, dictionary):
        return Similarity('~/Documents/', tfidf_model[corpus], num_features=len(dictionary))

    def _prepare_for_search(self):
        print('Processing documents...')
        processed_docs = self._processor.process_documents(self._documents)
        print('Creating dictionary...')
        self._dictionary = self._create_dictionary(processed_docs)
        print('Creating corpus...')
        self._corpus = self._create_corpus(self._dictionary, processed_docs)
        print('Creating tifidf model...')
        self._tifidf_model = self._create_tfidf_model(self._corpus)
        print('Creating similarity...')
        self._similarity = self._create_similarity(
            self._corpus, self._tifidf_model, self._dictionary)

    def search(self, doc):
        processed_doc = self._processor.process_document(doc)
        bow = self._dictionary.doc2bow(processed_doc)
        return self._similarity[self._tifidf_model[bow]]


class DocumentSearchExercise(object):

    def __init__(self):
        self._cleaner = DataCleaner()

    def search(self):
        with DBManager.get_engine().connect() as conn:
            externals = ExternosDao.select(conn)
            external_sn = self._cleaner.clean_external_clients(externals)
            external_names = self._cleaner.extract_names(external_sn)
            doc_search = DocumentSearch(external_names)
            internals = InternosDao.select(conn)
            internal_sn = self._cleaner.clean_internal_clients(internals)
            internal_names = self._cleaner.extract_names(internal_sn)
            for i in range(50):
                internal_name = random.choice(internal_names)
                similarity = doc_search.search(internal_name)
                print('For internal: {} similarity:{}'.format(
                    internal_name, similarity))


exercise = DocumentSearchExercise()
exercise.search()
