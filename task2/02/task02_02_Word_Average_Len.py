from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"\w+")

class MRWordAverageLen(MRJob):
    OUTPUT_PROTOCOL = TextProtocol
    
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):           
            yield len(word), 1

    # def combiner(self, word_len, value):
    #     yield sum(word_len),sum(value)

    def reducer(self, word_len, value):
        average = sum(word_len)/sum(value)
        yield average
   
        
if __name__ == '__main__':    
    MRWordAverageLen.run()
