import re

class Calculator:
	def __init__(self, pipe):
		self.data = pipe

	regexes = {
		"mail-security": { # spoofing, spam prevention
			"spf": [re.compile(r"v=spf1.*"), re.compile(r"spf1.*"), re.compile(r"spf2\.0.*")],
			"dkim": [re.compile(r"v=DKIM1.*", re.IGNORECASE)],
			"dmarc": [re.compile(r"v=DMARC1.*", re.IGNORECASE)],
		},
		"domain-identifiers": { # verifying ownership of the domain, so the services can work
			"google": [re.compile(r"google-site-verification.*")],
			"ms": [
				re.compile(r"MS.*", re.IGNORECASE), 
				re.compile(r".*onmicrosoft\.com"),
				re.compile(r"v=msv1.*"),
				re.compile(r"v=verifydomain.*")
			],
			"globalsign": [re.compile(r"_?globalsign-domain-verification.*")],
			"amazon": [re.compile(r"amazonses.*")],
			"yandex": [re.compile(r"yandex-verification.*")],
			"mailru": [re.compile(r"mailru.*")],
			"zoho": [re.compile(r"zoho-verification.*")],
			"adobe": [re.compile(r"adobe-idp-site-verification.*")],
			"sendinblue": [re.compile(r"Sendinblue-code.*")],
			"loaderio": [re.compile(r"loaderio.*")]
		},
		"keys": {
			"base64_64bytes": [re.compile(r"[A-z0-9+/]{86}==")], 
			"hex_16bytes": [re.compile(r"[A-Fa-f0-9]{32}")],
			"hex_40_42": [re.compile(r"[A-Fa-f0-9]{40,42}")],
			"dec_9_dec_678": [re.compile(r"\d{9}-\d{7,8}")],
			"rsa": [re.compile(r"k=rsa.*")]
		},
		"alias": [re.compile(r"ALIAS for.*")], # alias in TXT
		"i*m": [re.compile(r"i=\d{3}&m.*")], # no idea
		"malt": [re.compile(r"MAlt.*")], # 0 break something (usually mail)
		"number_pipe_site": [re.compile(r"\d\|.*")] # no idea
	}


	def size(self):
		return self.data.count()

	def get_sorted_remaining_records(self):
		return (
			self.data.map(lambda (type, name, text): (text, name))
			.sortByKey()
		)

	def remove_non_txt_records(self):
		self.data = self.data.filter(lambda (type, name, text): type == "TXT")

	def remove_empty_text_records(self):
		self.data = self.data.filter(lambda (type, name, text): text)
		
	# def remove_spf_records(self):
	# 	self.data = (
	# 		self.data.filter(lambda (type, name, text): )
	# 	)

	# def remove_verification_records(self):
	# 	self.data = (
	# 		self.data.filter(lambda (type, name, text): "google-site-verification" not in text)
	# 	)

	# def remove_ms_records(self):
	# 	self.data = (
	# 		self.data.filter(lambda (type, name, text): "MS=ms" not in text and "ms=ms" not in text)
	# 	)

	def get_specific_stats_for_regex_dictionary(self, dictionary):
		results = {}
		for key, value in dictionary.iteritems():
			if type(value) == dict:
				results[key] = self.get_specific_stats_for_regex_dictionary(value)
			else:
				results[key] = self.count_and_remove(value)
		return results

	def count_and_remove(self, regex_list):
		current_size = self.size()
		self.data = self.data.filter(lambda (type, name, text): not any (regex.match(text) for regex in regex_list))
		return current_size - self.size()