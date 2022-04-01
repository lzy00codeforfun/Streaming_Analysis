
from nltk.corpus import words
from nltk.corpus import wordnet
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# RT
# Hashtag
# @
# http:
# stopping words -> 过滤
# 非英文单词
def pre_processing_text(text):
    # RT & @
    text = text.split(" ")
    text = [word for word in text if word != "RT" or word[0] != "@"]
    # hashtag & http
    hashtag_list = []
    http_list = []
    new_text = []
    for word in text:
        if word.startswith("#"):
            hashtag_list.append(word)
        elif word.startswith("http"):
            http_list.append(word)
        else:
            new_text.append(word)
    # not english words
    text = " ".join(new_text)
    final_words = engproc.process(text)
    return final_words
    # stopping word & English filter
            
class EnglishProcess(object):
    def __init__(self,):
        super().__init__()
        self.stopWords = set(stopwords.words('english'))

    def process(self, text):
        if type(text) == str:
            text = word_tokenize(text)
        text = [word.lower() for word in text]

        # no plurals
        english_words = [word for word in text if word in words.words()]
        filtered_words = [word for word in text if word not in words.words()]

        # check plurals and sysnets -> english
        final_words = english_words + [word for word in filtered_words if wordnet.synsets(word)]
        
        if len(final_words) < 3: 
            return []
        print('final words english', final_words)

        wordsFiltered = []
        # stopping word
        for w in final_words:
            if w not in self.stopWords:
                wordsFiltered.append(w)
        
        if len(wordsFiltered) < 3:
            print('filtered ', wordsFiltered)
            return []
        
        return wordsFiltered
        
engproc = EnglishProcess()