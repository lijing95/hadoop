# from mrjob.job import MRJob
# # from mrjob.protocol import TextProtocol
# import re

# # WORD_RE = re.compile(r"[a-zA-Z]+")
# WORD_RE = re.compile(r"\w+")

# class MRWordUpper(MRJob):
#     # OUTPUT_PROTOCOL = TextProtocol
 
#     def mapper(self, _, line):
#         for word in WORD_RE.findall(line):
#             yield (word, 1)

#     def combiner(self, word, count):
#         yield  word,sum(count)

#     def combiner_2(self, word, counts):
#         yield  word[0].isupper(), (word,counts)
        

#     # def reducer(self, word, word_upper):
#     #     yield
            
# if __name__ == '__main__':    
#     MRWordUpper.run()  

from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"\w+")

# Все слова, которые более чем в половине случаев начинаются с большой буквы и встречаются больше 10 раз

class MRWordUpper(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), word[0].isupper()     #key - слово, value - true/false

    def reducer(self, word, is_upper):
        upper_count = 0     
        number_count = 0    
        for upper in is_upper:
            if upper:
                upper_count += 1
            number_count += 1

        case_count = number_count/2     
        if number_count > 10 and upper_count > case_count:      
            yield str(number_count), word           



if __name__ == '__main__':
    MRWordUpper.run()
    