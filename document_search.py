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


class DocumentSearch(object):

    def __init__(self, documents, id):
        self._processor = DocumentProcessor()
        self._documents = documents
        self._id = id
        self._processed_docs = None
        self._dictionary = None
        self._corpus = None
        self._tifidf_model = None
        self._similarity = None
        self._similarity = self._get_similarity()

    def _get_processed_docs(self):
        if not self._processed_docs:
            print('Processing documents...')
            self._processed_docs = self._processor.process_documents(
                self._documents)
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

    def _get_corpus(self):
        if not self._corpus:
            print('Creating corpus...')
            self._corpus = [self._doc2bow(doc)
                            for doc in self._get_processed_docs()]
        return self._corpus

    def _get_tfidf_model(self):
        if not self._tifidf_model:
            path = self._get_tfidf_path()
            if path.exists():
                print('Tfidf model already exists, loading from file...')
                self._tifidf_model = TfidfModel.load(str(path))
            else:
                print('Tfidf model does not exist.Creating...')
                self._tifidf_model = TfidfModel(self._get_corpus())
                self._tifidf_model.save(str(path))
        return self._tifidf_model

    def _get_data_path(self, ext):
        return pathlib.Path(__file__).parent / 'data/{}.{}'.format(self._id, ext)

    def _get_dictionary_path(self):
        return self._get_data_path('dict')

    def _get_tfidf_path(self):
        return self._get_data_path('tifidf')

    def _get_similarity_path(self):
        return self._get_data_path('sim')

    def _get_similarity_idx_path(self):
        return self._get_data_path('idx')

    def _get_similarity(self):
        if not self._similarity:
            path = self._get_similarity_path()
            if path.exists():
                print('Similarity already exists, loading from file...')
                self._similarity = Similarity.load(str(path))
            else:
                print('Similarity does not exist, creating...')
                self._similarity = Similarity(
                    str(self._get_similarity_idx_path()), self._get_tfidf_model()[self._get_corpus()], num_features=len(self._get_dictionary()))
                self._similarity.save(str(path))
        return self._similarity

    def search(self, doc, limit):
        processed_doc = self._processor.process_document(doc)
        bow = self._doc2bow(processed_doc)
        matches = self._get_similarity()[self._get_tfidf_model()[bow]]
        indexes = (matches > limit).nonzero()[0]
        results = [{'index': index, 'score': matches[index]}
                   for index in indexes]
        results.sort(key=lambda r: r['score'], reverse=True)
        return results


class DocumentSearchExercise(object):

    def __init__(self):
        self._cleaner = DataCleaner()

    def search(self):
        with DBManager.get_engine().connect() as conn:
            externals = ExternosDao.select(conn)
            external_sn = self._cleaner.clean_external_clients(externals)
            external_names = self._cleaner.extract_names(external_sn)
            doc_search = DocumentSearch(external_names, 'external_names')
            internals = InternosDao.select(conn)
            internal_sn = self._cleaner.clean_internal_clients(internals)
            num_internals = len(internal_sn)
            for i, internal in enumerate(internal_sn):
                internal_name = internal['nombre']
                if i % 500 == 0:
                    print('Progress: {}/{} ({:2%})', i,
                          num_internals, i/num_internals)
                results = doc_search.search(internal_name, .85)
                if len(results) > 0:
                    matches = []
                    print('For internal: {} Results:'.format(internal_name))
                    for result in results:
                        external = external_sn[result['index']]
                        external_name = external['nombre']
                        print(
                            '\t{} / Score: {}'.format(external_name, result['score']))
                        for clabe in external['clabes'].split(';'):
                            matches.append({
                                'nombre_interno': internal_name,
                                'nombre_externo': external_name,
                                'clabe_externo': clabe,
                                'agrupadores': internal['agrupadores'],
                                'score': result['score'].item()
                            })
                    PossibleMatchesDocDao.insert(conn, matches)


exercise = DocumentSearchExercise()
exercise.search()
