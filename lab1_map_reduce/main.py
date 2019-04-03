import string

from MapReduce import MapReduce
from RandomFileGenerator import RandomFileGenerator

if __name__ == "__main__":
    str_len_min = 100
    str_len_max = 255
    characters = string.ascii_letters + string.digits
    lines_count = 10 ** 9
    lines_max_in_file = 10 ** 7
    chunk_size = 2.5 * 10 ** 5
    num_processes = 4

    input_filename = "in.txt"
    output_filename = "out.txt"

    # Сгенерировать файл
    generator = RandomFileGenerator(str_len_min, str_len_max, characters, lines_count, lines_max_in_file,
                                    chunk_size, num_processes)
    generator.generate_file(input_filename)
    print("File generated")

    # Запустить MapReduce
    files_count = int(lines_count / lines_max_in_file)
    map_reduce = MapReduce(lines_max_in_file, characters, num_processes, files_count)
    map_reduce.execute(input_filename, output_filename)
    print("MapReduce executed")
