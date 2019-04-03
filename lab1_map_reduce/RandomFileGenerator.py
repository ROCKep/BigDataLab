import random


class RandomFileGenerator:

    def __init__(self, str_len_min, str_len_max, characters, lines_count):
        self.str_len_min = str_len_min
        self.str_len_max = str_len_max
        self.characters = characters
        self.lines_count = lines_count
        self.lines_max_in_file = lines_max_in_file
        self.chunk_size = chunk_size
        self.num_processes = num_processes

    def generate_file(self, filename):

        batch_size = int(self.lines_count / self.lines_max_in_file)
        chunk_count = int(self.lines_max_in_file / self.chunk_size)

        with open(filename, 'w') as fout:
            pool = Pool(processes=self.num_processes)
            for i in range(batch_size):
                chunk_list = pool.map(self._generate_chunk, range(chunk_count))
                fout.write(''.join(chunk_list))

    def _generate_chunk(self, idx):
        chunk = ''
        for i in range(self.chunk_size):
            chunk += self._generate_random_alphanum() + '\n'
        return chunk

    def _generate_random_alphanum(self):
        str_len = random.randint(self.str_len_min, self.str_len_max)
        line = self._generate_random_string(str_len)
        return line

    def _generate_random_string(self, line_size):
        result = ""
        for i in range(line_size):
            character = random.choice(self.characters)
            result += character
        return result
