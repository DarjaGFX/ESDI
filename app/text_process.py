import os
import re
import string
from enum import Enum
from typing import List
from dotenv import load_dotenv
from hazm import (Lemmatizer, Normalizer, POSTagger, Stemmer, WordTokenizer,
                  informal_normalizer, sent_tokenize, stopwords_list,
                  word_tokenize)

# alpha = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

load_dotenv()


def StopWords(files: List[str] = []):
    # return ['ali']
    stop_words = []
    stop_word_files_path = os.getenv(
        'STOPWORDS_DIRECTORY', default='.')
    if files != []:
        stop_word_files = files
    else:
        stop_word_files = ['chars', 'nonverbal',
                           'persian', 'stop_words', 'verbal']
    for stop_word_file in stop_word_files:
        with open(stop_word_files_path + stop_word_file, encoding="utf8") as stopWord:
            stop_words.extend(stopWord.read().split())
    stop_words = list(set(stop_words))
    return stop_words


class PreProcess:

    def __init__(self, text) -> None:
        self.text = text

    @classmethod
    def _Normal(cls, text):
        """
        ### Normalize Text
        """
        normalizer = Normalizer()
        return normalizer.normalize(text)

    def InformalNormal(self):
        """
        ## Informal Normalizer
        """
        normalizer = informal_normalizer.InformalNormalizer()
        infn = normalizer.normalize(self.text)
        newtext = ' '.join([i[0] for i in infn[0]])
        self.text = self._Normal(text=newtext)
        return self

    def RS(self, StopWords=StopWords()):
        """
        ## Remove StopWords
        """
        self.text = ' '.join(
            [word for word in self.text.split() if word not in StopWords])
        return self

    def Lem(self):
        """
        ## Lemmatize all words in Text
        """
        lemmatizer = Lemmatizer()
        self.text = ' '.join(
            [lemmatizer.lemmatize(word=word) for word in self.text.split()])
        return self

    def Stem(self):
        """
        ## Stemmize all words in Text
        """
        stemmer = Stemmer()
        self.text = ' '.join(
            [stemmer.stem(word=word) for word in self.text.split()])
        return self

    def GetVerbs(self):
        """
        ### clean self.text to contain only Verbs.
        """
        tagger = POSTagger(
            model=os.getenv('POS_TAGGER_DIRECTORY', default='.')+'pos_tagger.model')
        tagged = tagger.tag(WordTokenizer().tokenize(self.text))
        self.text = ' '.join(
            list(map(lambda x: x[0], filter(lambda x: x[1] == 'VERB', tagged))))
        return self

    def GetNouns(self):
        """
        ### clean self.text to contain only Nouns.
        """
        tagger = POSTagger(
            model=os.getenv('POS_TAGGER_DIRECTORY', default='.')+'pos_tagger.model')
        tagged = tagger.tag(WordTokenizer().tokenize(self.text))
        self.text = ' '.join(
            list(map(lambda x: x[0], filter(lambda x: x[1] == 'NOUN' or x[1] == 'NOUN,EZ', tagged))))
        return self

    def Rnf(self):
        """
        ## Remove Non-Farsi Chars
        removes [a-zA-z] chars from self.text
        """
        mt = str.maketrans(string.ascii_letters, ' '*len(string.ascii_letters))
        self.text = self.text.translate(mt)
        return self

    def Rpunc(self):
        """
        ## Remove punctuations
        removes punctuation chars from self.text
        """
        punctuation = string.punctuation.replace('_', '')
        mt = str.maketrans(punctuation, ' '*len(punctuation))
        self.text = self.text.translate(mt)
        return self

    def deEmojify(self):
        regrex_pattern = re.compile(pattern = "["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags = re.UNICODE)
        self.text = regrex_pattern.sub(r'',self.text)
        return self

def text_processor(document, search_mode: bool = False):
    punctuation = string.punctuation.replace('_', '') + '­'
    process_document = ''
    document = text_cleaner(document, search_mode=search_mode)
    Text = sent_tokenize(document)
    lemmatizer = Lemmatizer()
    stemmer = Stemmer()
    for txt in Text:
        tknz_txt = word_tokenize(txt)
        process_txt = ''
        for word in tknz_txt:
            word = word.strip(punctuation)
            word = word.replace('\xad', '')
            word = word.replace('\u200c', ' ')
            word = word.replace('\u00ad', '')
            word = word.replace('\N{SOFT HYPHEN}', '')
            if (len(word) > 1) and (word not in stopwords_list()):
                s_word = stemmer.stem(word)
                l_word = lemmatizer.lemmatize(s_word).split('#')[0]
                process_txt = process_txt + ' ' + l_word
        process_document = process_document + ' ' + process_txt
    return process_document


def text_cleaner(Text, search_mode: bool = False):
    """
    `search_mode`:
    `False` for Document insert Mode (remove all punctuations)
    `True` for Search Mode (keep + for use as OR)
    """
    # First we remove inline JavaScript/CSS:
    cleaned = re.sub(r"(?is)<(script|style).?>.?(</\1>)", "", Text.strip())
    # Then we remove html comments. This has to be done before removing regular
    # tags since comments can contain '>' characters.
    cleaned = re.sub(r"(?s)<!--(.*?)-->[\n]?", "", cleaned)
    # Next we can remove the remaining tags:
    cleaned = re.sub(r"(?s)<.*?>", " ", cleaned)
    # Finally, we deal with whitespace
    cleaned = re.sub(r"&nbsp;", " ", cleaned)
    cleaned = re.sub(r"  ", " ", cleaned)
    cleaned = re.sub(r"  ", " ", cleaned)
    cleaned = re.sub(r"  ", " ", cleaned)
    cleaned = re.sub(r"http\S+", "", cleaned)
    cleaned = cleaned.replace('&nbsp', ' ')
    cleaned = cleaned.replace('&zwnj', ' ')
    cleaned = cleaned.replace('&raquo', ' ')
    cleaned = cleaned.replace('&laquo', ' ')
    cleaned = cleaned.replace('&uarr', ' ')
    cleaned = cleaned.replace('&rlm', ' ')
    cleaned = cleaned.replace('&ndash', ' ')
    cleaned = cleaned.replace('\u200c', ' ')
    cleaned = cleaned.replace('\xad', '')
    cleaned = cleaned.replace('\u00ad', '')
    cleaned = cleaned.replace('\N{SOFT HYPHEN}', '')
    # Remove punctuations from Text
    punctuation = string.punctuation.replace('_', '')
    if search_mode:
        punctuation = punctuation.replace('+', '')
    d = {ord(c): None for c in punctuation}
    cleaned = cleaned.translate(d)
    # Remove Numbers
    nums = re.compile('[0-9]+')
    cleaned = re.sub(nums, '', cleaned)

    cleaned = cleaned.rstrip('\n')
    return cleaned.strip()
