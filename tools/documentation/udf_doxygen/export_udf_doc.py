import os
import subprocess
import yaml

DOXYGEN_DIR = os.path.abspath(os.path.dirname(__file__))
HOME_DIR = os.path.join(DOXYGEN_DIR, "../../..")
BUILD_DIR = os.path.abspath(os.path.join(HOME_DIR, "build"))
TMP_DIR = os.path.join(BUILD_DIR, "docs/tmp")
print(BUILD_DIR)

def export_yaml():
	if not os.path.exists(TMP_DIR):
		os.makedirs(TMP_DIR)
	ret = subprocess.call(
		[os.path.join(BUILD_DIR, "src/export_udf_info"),
		 "--output_dir", TMP_DIR,
		 "--output_file", "udf_defs.yaml"])
	if ret != 0:
		print("Invoke native export udf binary failed")
		exit(ret)


def process_doc(doc):
	lines = doc.split("\n")
	first_line_indent = -1;
	print(first_line_indent, lines[0])
	for i in range(0, len(lines)):
		line = lines[i]
		if line.strip() == "":
			continue
		if first_line_indent < 0:
			first_line_indent = len(line) - len(line.lstrip())
		indent = min(len(line) - len(line.lstrip()), first_line_indent)
		lines[i] = lines[i][indent: ]
	return "\n".join(lines)


def make_header():
	with open(os.path.join(TMP_DIR, "udf_defs.yaml")) as yaml_file:
		udf_defs = yaml.load(yaml_file.read())
	
	if not os.path.exists(DOXYGEN_DIR + "/udfs"):
		os.makedirs(DOXYGEN_DIR + "/udfs")
	fake_header = os.path.join(DOXYGEN_DIR + "/udfs/udfs.h")

	with open(fake_header, "w") as header_file:
		for name in sorted(udf_defs.keys()):
			content = "/**\n"

			content += "@par Description\n"
			items = udf_defs[name]
			print("Found %d registries for \"%s\"" % (len(items), name))
			for item in items:
				doc = item["doc"]
				if doc.strip() != "":
					content += process_doc(doc)
					break;
			content += "\n\n@par Supported Types\n"
			sig_list = []
			for item in items:
				arg_types = item["arg_types"]
				sig_list.append(", ".join(arg_types))
			sig_list = sorted(sig_list)
			for sig in sig_list:
				content += "- <" + sig + ">\n"

			content += "\n*/\n"
			content += name + "();\n"
			header_file.write(content)


def doxygen():
	ret = subprocess.call(["doxygen"], cwd=DOXYGEN_DIR)
	if ret != 0:
		print("Invoke doxygen failed")
		exit(ret)


if __name__ == "__main__":
	export_yaml()
	make_header()
	doxygen()
