import random


class RandomFileGenerator:

    def __init__(self, str_len_min, str_len_max, characters, lines_count):
        self.str_len_min = str_len_min
        self.str_len_max = str_len_max
        self.characters = characters
        self.lines_count = lines_count

    def generate_file(self, filename):
        with open(filename, 'a') as fout:
            for i in range(self.lines_count):
                fout.write(self._generate_random_alphanum() + '\n')

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
