import string

from MapReduce import MapReduce
from RandomFileGenerator import RandomFileGenerator

# Сгенерировать файл
generator = RandomFileGenerator(str_len_min=5, str_len_max=10, characters=string.ascii_lowercase, lines_count=100)
generator.generate_file(filename="in.txt")
print("File generated")

# Запустить MapReduce
map_reduce = MapReduce()
map_reduce.execute(input_filename="in.txt", output_filename="out.txt")
print("MapReduce executed")
