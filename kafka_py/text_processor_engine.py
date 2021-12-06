
from nltk.corpus import words
from nltk.corpus import wordnet
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import numpy as np
import os
import pickle
import time

            
class EnglishProcessor(object):
    def __init__(self):
        super().__init__()
        self.stopWords = set(stopwords.words('english')) # 179
        self.cnt = 0
        self.wordswords = words.words() # 236736
        self.debug = False
        self.load_vector_map_path() # 1055268
        self.vector_map_list = list(self.vector_map.keys())
        del self.vector_map

    def load_vector_map_path(self, path='vector_map.pkl'):
        # vectorization map
        if os.path.exists(path):
            print("load from path")
            with open(path, 'rb') as pkl_file:
                self.vector_map = pickle.load(pkl_file)
            print('load finished')
            return 

    def load_vector_map(self, path='vector_map.pkl'):
        print("load from scratch")
        self.load_vector_model()
        vector_map = {}
        # filter words start with @, http, #
        # filter words with the same lower letters (Hmf == hmf)
        # save the average vector
        word_list = self.vector_model.words
        word_list = [word for word in word_list if not word.startswith("@") \
                        and not word.startswith("http") and not word.startswith("#") \
                        and not word.startswith("&") and len(word) > 2]
        for word in word_list:
            word = word.lower()
            if word in vector_map:
                vector_map[word].append(self.vector_model[word])
            else:
                vector_map[word] = [self.vector_model[word]]
        
        for word in list(vector_map.keys()):
            vector_map[word] = np.array(vector_map[word]).mean(axis=0)
        
        print(len(list(vector_map.keys())))
        self.vector_map = vector_map
        with open(path, 'wb') as pkl_file:
            pickle.dump(vector_map, pkl_file)

    def load_vector_model(self):
        # vectorization map
        import fasttext
        vector_model = fasttext.load_model('fasttext_twitter_raw.bin')
        self.vector_model = vector_model

    def process(self, text):
        # RT
        # Hashtag
        # @
        # http:
        # stopping words -> 过滤
        # 非英文单词
        self.cnt = self.cnt + 1
        if self.debug:
            print('eng proc cnt', self.cnt)
        if self.debug:
            st = time.time()
        if type(text) == str:
            st_inner = time.time()
            text = word_tokenize(text)
            if self.debug:
                print("tokenize", time.time()-st_inner)
        text = [word.lower() for word in text]

        # no plurals
        if self.debug:
            st_inner = time.time()
        english_words = [word for word in text if word in self.wordswords]
        filtered_words = [word for word in text if word not in english_words]
        # with fasttext vocab 
        english_words = english_words + \
                        [word for word in filtered_words if word in self.vector_map_list]
        filtered_words = [word for word in text if word not in english_words]

        if self.debug:
            print("find words.words()", time.time()-st_inner)

        # check plurals and sysnets -> english
        if self.debug:
            st_inner = time.time()
        final_words = english_words + [word for word in filtered_words if wordnet.synsets(word)]
        if self.debug:
            print("find synsets", time.time()-st_inner)

        if len(final_words) < 1: 
            if self.debug:
                print('proc time', time.time()-st, len(text))
            return []
        # print('final words english', final_words)


        if self.debug:
            st_inner = time.time()
        wordsFiltered = []
        # stopping word
        for w in final_words:
            if w not in self.stopWords:
                wordsFiltered.append(w)
        if self.debug:
            print("stopped word", time.time()-st_inner)

        if len(wordsFiltered) < 3:
            if self.debug:
                print('filtered ', wordsFiltered)
                print('proc time', time.time()-st, len(text))
            return []
        
        if self.debug:
            print('proc time', time.time()-st, len(text))
        return wordsFiltered
    
    def get_vector_map(self):
        
        return self.vector_map
    
    def get_vector_model(self):
        
        return self.vector_model
        
engtextproc = EnglishProcessor()