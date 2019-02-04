import pathlib
import random
import re
from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim.similarities import Similarity
from fuzzywuzzy import fuzz
from db_manager import DBManager
from data_cleaner import DataCleaner
from document_processor import DocumentProcessor
from dao.externos_sn_dao import ExternosSNDao
from dao.internos_sn_dao import InternosSNDao
from dao.possible_matches_doc_ext_idx_dao import PossibleMatchesDocExtIdxDao
from dao.possible_matches_doc_int_idx_dao import PossibleMatchesDocIntIdxDao


class DocumentSearch(object):

    def __init__(self, documents, id, domain_terms=[]):
        self._processor = DocumentProcessor()
        self._cleaner = DataCleaner()
        self._documents = documents
        self._domain_terms = [(re.compile(r'\b'+domain_term+r'\b'), '')
                              for domain_term in domain_terms]
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

    def search(self, doc, similarity_limit, in_dict_limit):
        results = []
        processed_doc = self._processor.process_document(doc)
        in_dict_percentage = self._calculate_in_dict_percentage(
            processed_doc)
        if in_dict_percentage < in_dict_limit:
            return results
        bow = self._doc2bow(processed_doc)
        matches = self._get_similarity()[self._get_tfidf_model()[bow]]
        indexes = (matches > (similarity_limit/100)).nonzero()[0]
        domain_terms = self._domain_terms_in_doc(doc)
        stripped_doc = self._cleaner.replace_terms(doc, domain_terms)

        for index in indexes:
            similarity = matches[index].item()
            match = self._documents[index]
            if len(domain_terms) > 0:
                match = self._cleaner.replace_terms(
                    match, domain_terms)
            token_set_ratio = fuzz.token_set_ratio(
                stripped_doc, match)
            if token_set_ratio >= similarity_limit:
                partial_ratio = fuzz.partial_ratio(stripped_doc, match)
                stripped_length = len(match)
                results = [{
                    'index': index,
                    'token_set_ratio': token_set_ratio,
                    'similarity': similarity,
                    'partial_ratio': partial_ratio,
                    'in_dict_percentage': in_dict_percentage,
                    'stripped_length': stripped_length,
                    'total': (token_set_ratio * 100) + (partial_ratio * 10) + stripped_length
                }]

        results.sort(key=lambda r: r['total'], reverse=True)
        return results

    def _domain_terms_in_doc(self, doc):
        domain_terms = [
            term for term in self._domain_terms if term[0].search(doc)]
        return domain_terms

    def _count_words_in_dict(self, processed_doc):
        count = 0
        token2id = self._get_dictionary().token2id
        for word in processed_doc:
            if word in token2id:
                count += 1
        return count

    def _calculate_in_dict_percentage(self, processed_doc):
        return self._count_words_in_dict(processed_doc)/len(processed_doc)


class DocumentSearchExercise(object):

    def __init__(self):
        self._cleaner = DataCleaner()
        self._domain_terms = [
            'bank',
            'services',
            'fondo',
            'national',
            'grupo',
            'ahorro',
            'corporation',
            'international',
            'inc',
            'inmobilaria',
            'company',
            'operadora',
            'comercial',
            'industrial'
        ]

    def search(self, external_idx=False):
        with DBManager.get_engine().connect() as conn:
            if external_idx:
                self._searchExternalIdx(conn)
            else:
                self._searchInternalIdx(conn)

    def _searchExternalIdx(self, conn):
        external_sn = ExternosSNDao.select(conn)
        external_names = self._cleaner.extract_names(external_sn)
        doc_search = DocumentSearch(
            external_names, 'external_names', domain_terms=self._domain_terms)
        internal_sn = InternosSNDao.select(conn)
        num_internals = len(internal_sn)
        for i, internal in enumerate(internal_sn):
            internal_name = internal['nombre']
            if i % 500 == 0:
                print('Progress: {}/{} ({:2%})'.format(i,
                                                       num_internals, i/num_internals))
            results = doc_search.search(internal_name, 85, 0.34)
            if len(results) > 0:
                matches = []
                print('For internal: {} Results:'.format(internal_name))
                for result in results:
                    external = external_sn[result['index']]
                    print(
                        '\t{} / Token set ratio: {}, Partial ratio: {}, In dict percentage:{},  '.format(external['nombre'], result['token_set_ratio'], result['partial_ratio'], result['in_dict_percentage']))
                    matches += self._createMatchObjs(internal,
                                                     external, result)
                PossibleMatchesDocExtIdxDao.insert(conn, matches)

    def _searchInternalIdx(self, conn):
        PossibleMatchesDocIntIdxDao.truncate(conn)
        internal_sn = InternosSNDao.select(conn)
        internal_names = self._cleaner.extract_names(internal_sn)
        doc_search = DocumentSearch(
            internal_names, 'internal_names', domain_terms=self._domain_terms)
        external_sn = ExternosSNDao.select(conn)
        num_externals = len(external_sn)
        for i, external in enumerate(external_sn):
            external_name = external['nombre']
            if i % 500 == 0:
                print('Progress: {}/{} ({:2%})'.format(i,
                                                       num_externals, i/num_externals))
            results = doc_search.search(external_name, 85, 0.34)
            if len(results) > 0:
                matches = []
                print('For external: {} Results:'.format(external_name))
                for result in results:
                    internal = internal_sn[result['index']]
                    print(
                        '\t{} / Token set ratio: {}, Partial ratio: {}, In dict percentage:{},  '.format(internal['nombre'], result['token_set_ratio'], result['partial_ratio'], result['in_dict_percentage']))
                    matches += self._createMatchObjs(internal,
                                                     external, result)
                PossibleMatchesDocIntIdxDao.insert(conn, matches)

    def _createMatchObjs(self, internal, external, result):
        matches = []
        for clabe in external['clabes'].split(';'):
            matches.append({
                'nombre_interno': internal['nombre'],
                'nombre_externo': external['nombre'],
                'clabe_externo': clabe,
                'agrupador_ids': internal['agrupador_ids'],
                'token_set_ratio': result['token_set_ratio'],
                'similarity': result['similarity'],
                'partial_ratio': result['partial_ratio'],
                'in_dict_percentage': result['in_dict_percentage'],
                'stripped_length': result['stripped_length'],
                'total': result['total']
            })
        return matches


exercise = DocumentSearchExercise()
exercise.search()
