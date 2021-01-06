from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r'(?:[A-Za-zА-Яа-я]\. ?[а-яa-z]\.)+')
class MRWordTask6(MRJob):
    OUTPUT_PROTOCOL = TextProtocol 
  
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1
               
    def combiner(self, word, counts):
        yield None, (sum(counts), word)
    #
    def reducer(self, _, word_count_pairs):
        pairs = list(word_count_pairs) 
        for counts, word in pairs:
            word_freq = counts/len(pairs) 
            if word_freq > 0.02:   
                yield word, str(word_freq)
        

if __name__ == "__main__":
    MRWordTask6.run() 