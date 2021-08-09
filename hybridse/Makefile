all:
	@echo working on it

lint: cpplint shlint javalint pylint

format: javafmt shfmt cppfmt pyfmt configfmt

javafmt:
	@cd java && mvn -pl hybridse-sdk spotless:apply

shfmt:
	@if command -v shfmt; then\
		git ls-files | grep --regexp "\.sh$$" | xargs -I {} shfmt -i 4 -w {}; \
		exit 0; \
	else \
		echo "SKIP: shfmt (shfmt not found)"; \
	fi

cppfmt:
	@if command -v clang-format; then \
		git ls-files | grep --regexp "\(\.h\|\.cc\)$$" | xargs -I {} clang-format -i -style=file {} ; \
		exit 0; \
	else \
		echo "SKIP: cppfmt (clang-format not found)"; \
	fi

pyfmt:
	@if command -v yapf; then \
		git ls-files | grep --regexp "\.py$$" | xargs -I {} yapf -i --style=google {}; \
		exit 0; \
	else \
		echo "SKIP: pyfmt (yapf not found)"; \
	fi

configfmt: yamlfmt jsonfmt

yamlfmt:
	@if command -v prettier; then \
		git ls-files | grep --regexp "\(\.yaml\|\.yml\)$$" | xargs -I {} prettier -w {}; \
		exit 0; \
	else \
		echo "SKIP: yamlfmt (prettier not found)"; \
	fi

jsonfmt:
	@if command -v prettier; then \
		git ls-files | grep --regexp "\.json$$" | xargs -I {} prettier -w {}; \
		exit 0; \
	else \
		echo "SKIP: jsonfmt (prettier not found)"; \
	fi

xmlfmt:
	@if command -v prettier; then \
		git ls-files | grep --regexp "\.xml$$" | xargs -I {} prettier --plugin=@prettier/plugin-xml --plugin-search-dir=./node_modules -w {}; \
		exit 0; \
	else \
		echo "SKIP: xmlfmt (prettier not found)"; \
	fi


cpplint:
	@if command -v cpplint; then \
		git ls-files | grep --regexp "\(\.h\|\.cc\)$$" | xargs -I {} cpplint {} ; \
	else \
		echo "SKIP: cpplint (cpplint not found)"; \
	fi

shlint:
	@if command -v shellcheck; then \
		git ls-files | grep --regexp "\.sh$$" | xargs -I {} shellcheck {}; \
	else \
		echo "SKIP: shlint (shellcheck not found)"; \
	fi

javalint:
	@cd java && mvn -pl hybridse-sdk -Dplugin.violationSeverity=warning checkstyle:check

pylint:
	@if command -v pylint; then \
		git ls-files | grep --regexp "\.py$$" | xargs -I {} pylint {}; \
	else \
		echo "SKIP: pylint (pylint not found)"; \
	fi
