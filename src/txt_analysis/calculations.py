import re
from operator import add

def label_record((type, name, text)):
	text = re.sub("\"", "", str(text)).strip()
	if type == "TXT":
		if text:
			return [("dns", 1), ("correct_txt", 1), ("txt", 1)]
		else:
			return [("dns", 1), ("txt", 1)]
	else:
		return [("dns", 1)]

def match_for_regex_dictionary(text, dictionary, prefix=""):
	matches = []
	for key, value in dictionary.iteritems():
		if type(value) == dict:
			matches += match_for_regex_dictionary(text, value, key + ".")
		else:
			if matches_regex(text, value):
				matches.append((prefix + key, 1))
	return matches

def matches_regex(text, regex_list):
	return any (regex.match(text) for regex in regex_list)

regexes = {
	"mail-security": { # spoofing, spam prevention
		"spf": [
			re.compile(r"\\?v=spf[123]?.*", re.IGNORECASE),
			re.compile(r"include:.*", re.IGNORECASE),
			re.compile(r"spf1.*"),
			re.compile(r"spf2\.0.*")],
		"dkim": [re.compile(r"v=DKIM1.*", re.IGNORECASE)],
		"dmarc": [re.compile(r"v=DMARC1.*", re.IGNORECASE)],
	},
	"domain-identifiers": { # verifying ownership of the domain, so the services can work
		"google": [re.compile(r"_?google-site-verification.*")],
		"ms": [
			re.compile(r"MS.*", re.IGNORECASE), 
			re.compile(r".*onmicrosoft\.com$"),
			re.compile(r"v=msv1.*"),
			re.compile(r"v=verifydomain.*")
		],
		"amazon": [re.compile(r"_?amazonses.*")],
		"yandex": [re.compile(r"yandex-verification.*")],
		"mailru": [re.compile(r"mailru.*")],
		"zoho": [re.compile(r"zoho-verification.*")],
		"adobe": [re.compile(r"adobe-idp-site-verification.*")],
		"sendinblue": [re.compile(r"Sendinblue-code.*")],
		"loaderio": [re.compile(r"loaderio.*")],
		"atlassian": [re.compile(r"atlassian-domain-verification.*")],
		"citrix": [re.compile(r"citrix.*")],
		"facebook": [re.compile(r"facebook-domain-verification.*")],
		"pardot": [re.compile(r"pardot_.*")],
		"status_page": [re.compile(r"status-page-domain-verification.*")],
		"proofpoint": [re.compile(r"ppe-.*")],
		"firebase": [re.compile(r"firebase.*")],
		"dropbox": [re.compile(r"dropbox-domain-verification.*")]
	},
	"keys": {
		"base64_64bytes": [re.compile(r"[A-z0-9+/]{86}==$")],
		"base64_32bytes": [re.compile(r"[A-z0-9+/]{43}=$")],
		"hex_16bytes": [re.compile(r"[A-Fa-f0-9]{32}$")],
		"hex_32bytes": [re.compile(r"[A-Fa-f0-9]{64}$")],
		"hex_40_42": [re.compile(r"[A-Fa-f0-9]{40,42}$")],
		"hex_8-4-4-4-12": [
			re.compile(r"[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}$")
		],
		"dec_9_dec_678": [re.compile(r"\d{9}-\d{6,8}$")],
		"rsa": [re.compile(r"k=rsa.*")],
		"az_09_26": [re.compile(r"[A-z0-9]{26}$")]
	},
	"signature-services": {
		"docusign": [re.compile(r"docusign.*")],
		"globalsign": [re.compile(r"_?globalsign-domain-verification.*")],
	},
	"alias": [re.compile(r"ALIAS for.*")], # alias in TXT
	"i*m": [re.compile(r"i=\d{3}&m.*")], # no idea
	"malt": [re.compile(r"MAlt.*")], # 0 break something (usually mail)
	"number_pipe_site": [re.compile(r"\d\|.*")] # no idea
}

sensitive_tags = ["login", "user", "password", "pswd", "private"]

class Calculator:
	def __init__(self, pipe):
		self.data = pipe

	def count_basic_stats(self):
		return self.data.flatMap(label_record).reduceByKey(add)

	def filter_data(self):
		self.data = (
			self.data
			.filter(lambda (type, name, text): type == "TXT")
			.map(lambda (type, name, text): (re.sub("\"", "", str(text)).strip(), name))
			.filter(lambda (text, name): text)
		)

	def get_specific_stats(self):
		return (
			self.data
			.flatMap(lambda (text, name): match_for_regex_dictionary(text, regexes))
			.reduceByKey(add)
		)

	def get_sensitive_data(self):
		return (
			self.data
			.filter(lambda (text, name): any (re.search(tag, text, re.IGNORECASE) for tag in sensitive_tags))
		)

