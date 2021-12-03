
from nltk.corpus import words
from nltk.corpus import wordnet
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import time

            
class EnglishProcessor(object):
    def __init__(self,):
        super().__init__()
        self.stopWords = set(stopwords.words('english')) # 179
        self.cnt = 0
        self.wordswords = words.words() # 236736
        self.debug = False
        self.load_vector_model()
        self.load_vector_map() # 1055268

    def load_vector_map(self,):
        # vectorization map
        vector_map = {}
        # filter words start with @, http, #
        # filter words with the same lower letters (Hmf == hmf)
        # save the average vector
        self.vector_model.words
        self.vector_map = vector_map

    def load_vector_model(self,):
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
        if self.debug:
            print("find words.words()", time.time()-st_inner)

        # check plurals and sysnets -> english
        if self.debug:
            st_inner = time.time()
        final_words = english_words + [word for word in filtered_words if wordnet.synsets(word)]
        if self.debug:
            print("find synsets", time.time()-st_inner)

        if len(final_words) < 2: 
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