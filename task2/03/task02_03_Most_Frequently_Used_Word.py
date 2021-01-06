# from mrjob.job import MRJob
# import re

# WORD_RE = re.compile(r"[a-zA-Z]+")

# class MRMostFrequentlyUsedWord(MRJob):

#     def mapper(self, _, line):
#         for word in WORD_RE.findall(line):  
#             word = word.lower()         
#             yield (word , 1)

#     def combiner(self, word, count):
#         yield None,(sum(count),word)
  
#     def reducer(self, word_count):
#        yield max(word_count)
   
        
# if __name__ == '__main__':    
#     MRMostFrequentlyUsedWord.run()



    
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[a-zA-Z]+")

class MRMostFrequentlyUsedWord(MRJob):

    
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):          
            yield (word.lower() , 1)
  
    def combiner(self, words, counts):
        yield None, (sum(counts),words)

    def reducer(self, _, word_count):
            yield max(word_count)
        
if __name__ == '__main__':    
    MRMostFrequentlyUsedWord.run()