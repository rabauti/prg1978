import re
from tqdm import tqdm


from .base_reader import BaseReader
from .syntax_graph import SyntaxGraph

"""
0 word - tokens
1 lemma
2 syn_id - the sequence number of the word itself
3 pos
4 form
5 features
6 syn_rel - UD categories (syntactic relations)
7 syn_head - the sequence number of the head word
"""

class TasakReader(BaseReader):
	__FILE = None

	def __init__(self, file_name ):
		self.__FILE = file_name
		super().__init__()

	def get_sentences(self, mode='graph'):
		print(self.__FILE)
		self.log_info('Reading sentences in progress.')
		if mode not in ['graph', 'text']:
			raise Exception("Unknown mode %s", mode)
		# global line counter
		count = 0
		doc = None
		with open(self.__FILE) as f:
			# current sentence data
			current_sentence = []
			# sentence graph object
			# current collection id
			col_id = 0
			# prev collection id
			prev_col = None
			# global sentence id - collid_sentensespanstart_sentencecountincollection
			global_sent_id = None
			# sentence counter in collection
			coll_sentence_counter = 0
			sentence_span_start = 0
			# total lines in TSV file for progressbar calculations
			total_lines = self.count_lines()
			for line in tqdm(f, total=total_lines, desc='TSV lines'):
				unsaved = 1
				count += 1
				line = line.strip('\r\n')
				row = line.split('\t')
				if len(row) == 1 and line.startswith('<'):
					sentence_match = re.match(r'<s( id="([\d]+)")*>$', line)
					if sentence_match:
						col_id += 1
						current_sentence = []
						global_sent_id = sentence_match.group(2)
					elif line.startswith('<doc '):
						doc = re.sub(r'^<doc +', '', line)
					elif line == '</s>':
						g = SyntaxGraph(current_sentence)
						if mode == 'graph':
							g.set_metadata('doc', doc)
							yield global_sent_id, g
						else:
							yield global_sent_id, current_sentence_text
					continue
				elif not len(row) == 8:
					self.log_error(line)
					raise Exception(f'Wrong columns number line number {count} in TSV {self.__FILE}')
				data = {}
				if not prev_col == col_id:
					coll_sentence_counter = 0
				prev_col = col_id
				prev_global_sent_id = global_sent_id
				prev_sentence_span_start = sentence_span_start

				data['id'] = int(row[2])
				data['form'] = row[0]
				data['lemma'] = row[1]
				data['upostag'] = row[3]
				data['deprel'] = row[6]
				data['head'] = int(row[7])
				data['feats'] = {value: value for value in row[5].split('_')}
				data['verbform'] = None
				if data['upostag'] in 'V':
					data['verbform'] = row[4].split('.')[-1]

				current_sentence.append(data)


	def count_lines(self):
		def blocks(files, size=65536):
			while True:
				b = files.read(size)
				if not b: break
				yield b

		with open(self.__FILE, "r", encoding="utf-8", errors='ignore') as f:
			return sum(bl.count("\n") for bl in blocks(f))
