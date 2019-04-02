import os
import string
from multiprocessing.pool import Pool


class MapReduce:

    def execute(self, input_filename, output_filename):
        # Разделить файл для мапперов
        self._split_file(input_filename, "map_in", 5)

        # Запустить мапперы
        map_in = ["map_in/{}".format(filename) for filename in os.listdir("map_in")]
        map_out = ["map_out/{}".format(os.path.split(filename)[1]) for filename in map_in]
        map_args = zip(map_in, map_out)
        pool = Pool(processes=4)
        pool.starmap(self._map, map_args)

        # Запустить редусеры
        characters = string.ascii_letters + string.digits
        reduce_in = ["map_out"] * len(characters)
        reduce_out = ["reduce_out/{:02d}.txt".format(i) for i in range(len(characters))]
        reduce_args = zip(reduce_in, reduce_out, characters)
        pool.starmap(self._reduce, reduce_args)

        # Объединить файлы редусеров
        self._combine_files("reduce_out", output_filename)

    def _split_file(self, input_filename, output_folder, files_count):
        lines_max = 20
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        with open(input_filename, 'r') as fin:
            for i in range(files_count):
                lines = []
                for j in range(lines_max):
                    lines += fin.readline()
                with open(os.path.join(output_folder, "{}.txt".format(i)), 'w') as fout:
                    for line in lines:
                        fout.write(line)

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
            for entry in mapped:
                fout.write("{},{}\n".format(entry[0], entry[1]))

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
            for (line, count) in reduced:
                fout.write("{},{}\n".format(line, str(count)))

    def _combine_files(self, input_folder, output_filename):
        input_filenames = os.listdir(input_folder)
        with open(output_filename, 'w') as fout:
            for input_filename in input_filenames:
                with open(os.path.join(input_folder, input_filename), 'r') as fin:
                    for line in fin:
                        fout.write(line)
