class MapReduce:

    def map(self, input_filename, output_filename):
        # Открыть файл
        with open(input_filename, 'r') as fin:
            mapped = []
            # Для каждой строки поставить 1
            for line in fin:
                mapped.append((line.rstrip('\n'), 1))

        # Записать файл
        with open(output_filename, 'w') as fout:
            for entry in mapped:
                fout.write("{},{}\n".format(entry[0], entry[1]))

    def reduce(self, input_filename, output_filename):
        reduced = {}
        # Открыть файл
        with open(input_filename, 'r') as fin:
            for line_count in fin:
                line, count = line_count.rstrip('\n').split(',')
                # Сгруппировать одинаковые строки и просуммировать количество повторяющихся строк
                if line not in reduced:
                    reduced[line] = 0
                reduced[line] += int(count)

        # Отсортировать по алфавиту
        reduced = sorted(reduced.items())

        # Записать в файл
        with open(output_filename, 'w') as fout:
            for (line, count) in reduced:
                fout.write("{},{}\n".format(line, str(count)))
