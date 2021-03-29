all:
	@echo working on it

lint: cpplint shlint javalint pylint

format: javafmt shfmt cppfmt pyfmt configfmt

javafmt:
	@git ls-files | grep --regexp "\.java$$" | xargs -I {} google-java-format --aosp -i {}

shfmt:
	@git ls-files | grep --regexp "\.sh$$" | xargs -I {} shfmt -i 4 -w {}

cppfmt:
	@git ls-files | grep --regexp "\(\.h\|\.cc\)$$" | xargs -I {} clang-format -i -style=file {}

pyfmt:
	@git ls-files | grep --regexp "\.py$$" | xargs -I {} yapf -i --style=google {}

configfmt: yamlfmt jsonfmt xmlfmt

yamlfmt:
	@git ls-files | grep --regexp "\(\.yaml\|\.yml\)$$" | xargs -I {} prettier -w {}

jsonfmt:
	@git ls-files | grep --regexp "\.json$$" | xargs -I {} prettier -w {}

xmlfmt:
	@git ls-files | grep --regexp "\.xml$$" | xargs -I {} prettier -w {}


cpplint:
	@git ls-files | grep --regexp "\(\.h\|\.cc\)$$" | xargs -I {} cpplint {}

shlint:
	@git ls-files | grep --regexp "\.sh$$" | xargs -I {} shellcheck {}

javalint:
	@cd java && mvn -pl hybridse-common checkstyle:check

pylint:
	@git ls-files | grep --regexp "\.py$$" | xargs -I {} pylint {}
