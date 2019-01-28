import os
import time
import math
import re
from nltk.tokenize import word_tokenize


class DocumentProcessor(object):

    def process_document(self, document):
        return word_tokenize(document)

    def process_documents(self, documents):
        return [self.process_document(document) for document in documents]
