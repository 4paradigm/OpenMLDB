all:
	@echo working on it

lint: cpplint shlint javalint pylint

format: javafmt shfmt cppfmt pyfmt configfmt

javafmt:
	@command -v google-java-format && git ls-files | grep --regexp "\.java$$" | xargs -I {} google-java-format --aosp -i {} \
		|| echo "SKIP: javafmt (google-java-format not found)"

shfmt:
	@command -v shfmt && git ls-files | grep --regexp "\.sh$$" | xargs -I {} shfmt -i 4 -w {} \
		|| echo "SKIP: shfmt (shfmt not found)"

cppfmt:
	@command -v clang-format && git ls-files | grep --regexp "\(\.h\|\.cc\)$$" | xargs -I {} clang-format -i -style=file {} \
		|| echo "SKIP: cppfmt (clang-format not found)"

pyfmt:
	@command -v yapf && git ls-files | grep --regexp "\.py$$" | xargs -I {} yapf -i --style=google {} \
		|| echo "SKIP: pyfmt (yapf not found)"

configfmt: yamlfmt jsonfmt xmlfmt

yamlfmt:
	@command -v prettier && git ls-files | grep --regexp "\(\.yaml\|\.yml\)$$" | xargs -I {} prettier -w {} \
		|| echo "SKIP: yamlfmt (prettier not found)"

jsonfmt:
	@command -v prettier && git ls-files | grep --regexp "\.json$$" | xargs -I {} prettier -w {} \
		|| echo "SKIP: jsonfmt (prettier not found)"

xmlfmt:
	@command -v prettier \
		&& git ls-files | grep --regexp "\.xml$$" | xargs -I {} prettier --plugin=@prettier/plugin-xml --plugin-search-dir=./node_modules -w {} \
		|| echo "SKIP: xmlfmt (prettier not found)"


cpplint:
	@command -v cpplint && git ls-files | grep --regexp "\(\.h\|\.cc\)$$" | xargs -I {} cpplint {} \
		|| echo "SKIP: cpplint (cpplint not found)"

shlint:
	@command -v shellcheck && git ls-files | grep --regexp "\.sh$$" | xargs -I {} shellcheck {} \
		|| echo "SKIP: shlint (shellcheck not found)"

javalint:
	@cd java && mvn -pl hybridse-common -Dplugin.violationSeverity=warning checkstyle:check

pylint:
	@command -v pylint && git ls-files | grep --regexp "\.py$$" | xargs -I {} pylint {} \
		echo "SKIP: pylint (pylint not found)"
