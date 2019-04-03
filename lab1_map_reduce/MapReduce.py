import os
from multiprocessing.pool import Pool


class MapReduce:
    map_in_folder = "map_in"
    map_out_folder = "map_out"
    reduce_out_folder = "reduce_out"

    def __init__(self, lines_max_in_file, characters, num_processes, files_count):
        self.lines_max_in_file = lines_max_in_file
        self.characters = characters
        self.num_processes = num_processes
        self.files_count = files_count

    def execute(self, input_filename, output_filename):
        # Разделить файл для мапперов
        self._split_file(input_filename, self.map_in_folder, self.files_count)

        # Запустить мапперы
        map_in = [os.path.join(self.map_in_folder, filename) for filename in os.listdir(self.map_in_folder)]
        map_out = [os.path.join(self.map_out_folder, os.path.split(filename)[1]) for filename in map_in]
        map_args = zip(map_in, map_out)
        pool = Pool(processes=self.num_processes)
        pool.starmap(self._map, map_args)

        # Запустить редусеры
        reduce_in = [self.map_out_folder] * len(self.characters)
        reduce_out = ["{}/{:02d}.txt".format(self.reduce_out_folder, i) for i in range(len(self.characters))]
        reduce_args = zip(reduce_in, reduce_out, self.characters)
        pool.starmap(self._reduce, reduce_args)

        # Объединить файлы редусеров
        self._combine_files(self.reduce_out_folder, output_filename)

    def _split_file(self, input_filename, output_folder, files_count):
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        with open(input_filename, 'r') as fin:
            for i in range(files_count):
                lines = []
                for j in range(self.lines_max_in_file):
                    lines += fin.readline()
                with open(os.path.join(output_folder, "{}.txt".format(i)), 'w') as fout:
                    fout.write(''.join([line for line in lines]))

    def _map(self, input_filename, output_filename):
        # Открыть файл
        with open(input_filename, 'r') as fin:
            mapped = []
            # Для каждой строки поставить 1
            for line in fin:
                mapped.append((line.rstrip('\n'), 1))

        # Записать файл
        output_folder, _ = os.path.split(output_filename)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        with open(output_filename, 'w') as fout:
            fout.write(''.join(["{},{}\n".format(entry[0], entry[1]) for entry in mapped]))

    def _reduce(self, input_folder, output_filename, character):
        reduced = {}
        # Открыть файлы
        input_filenames = os.listdir(input_folder)
        for input_filename in input_filenames:
            with open(os.path.join(input_folder, input_filename), 'r') as fin:
                for line_count in fin:
                    line, count = line_count.rstrip('\n').split(',')
                    # Выбрать нужные строки
                    if line.startswith(character):
                        # Сгруппировать одинаковые строки и просуммировать количество повторяющихся строк
                        if line not in reduced:
                            reduced[line] = 0
                        reduced[line] += int(count)

        # Отсортировать по алфавиту
        reduced = sorted(reduced.items())

        # Записать в файл
        output_folder, _ = os.path.split(output_filename)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        with open(output_filename, 'w') as fout:
            fout.write(''.join(["{},{}\n".format(line, count) for line, count in reduced]))

    def _combine_files(self, input_folder, output_filename):
        input_filenames = os.listdir(input_folder)
        with open(output_filename, 'w') as fout:
            for input_filename in input_filenames:
                with open(os.path.join(input_folder, input_filename), 'r') as fin:
                    fout.write(''.join([line for line in fin]))
