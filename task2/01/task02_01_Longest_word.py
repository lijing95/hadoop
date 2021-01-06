from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\w+")

class MRLongestWord(MRJob):
    
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):           
            yield len(word), word

    def combiner(self, word_len, word):
        yield None, (word_len, ','.join(word))

    def reducer(self, _, word_len):
        yield max(word_len)
   
        
if __name__ == '__main__':    
    MRLongestWord.run()
