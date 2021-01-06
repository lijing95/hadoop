from mrjob.job import MRJob
import re

WORD_RE = re.compile(r'(?:[А-Яа-яA-za-z])[А-Яа-яA-Za-z]*\.')

class MRWordTask5(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner(self, word, count):
        yield None, (sum(count), word)      

    def reducer(self, _, pairs): 
        sorted_pairs = sorted(pairs)       

        for count, word in sorted_pairs:
            percent = count / len(sorted_pairs)    
            if percent > 0.01:
                yield word, str(percent)

if __name__ == '__main__':

    MRWordTask5.run()