class Calculator:
	def __init__(self, pipe):
		self.data = pipe

	def size(self):
		return self.data.count()

	def remove_non_txt_records(self):
		self.data = self.data.filter(lambda (type, name, text): type == "TXT")

	def remove_empty_text_records(self):
		self.data = self.data.filter(lambda (type, name, text): text)
		