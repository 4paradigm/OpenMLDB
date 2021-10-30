# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR  := $(dir $(MAKEFILE_PATH))
NPROC := $(shell (nproc))

CMAKE_PRG ?= $(shell (command -v cmake3 || echo cmake))
CMAKE_BUILD_TYPE ?= Release
CMAKE_FLAGS := -DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE)


all:
	@echo working on it: $(MAKEFILE_DIR) : $(shell (nproc))
	@echo cmake_build_type: $(CMAKE_BUILD_TYPE)

OPENMLDB_BUILD_DIR := $(MAKEFILE_DIR)/build

openmldb-build: openmldb-configure
	$(CMAKE_PRG) --build $(OPENMLDB_BUILD_DIR) -- -j$(NPROC)

openmldb-configure: hybridse-install
	$(CMAKE_PRG) -S . -B $(OPENMLDB_BUILD_DIR) $(CMAKE_FLAGS)

openmldb-clean:
	rm -rf "$(OPENMLDB_BUILD_DIR)"

THIRD_PARTY_BUILD_DIR := $(MAKEFILE_DIR)/.deps
THIRD_PARTY_SRC_DIR := $(MAKEFILE_DIR)/thirdsrc
THIRD_PARTY_DIR := $(THIRD_PARTY_BUILD_DIR)/usr

# third party compiled code install to 'OpenMLDB/.deps/usr', source code install to 'OpenMLDB/thirdsrc'
third-party: third-party-configure
	$(CMAKE_PRG) --build .deps

third-party-configure:
	$(CMAKE_PRG) -S third-party -B $(THIRD_PARTY_BUILD_DIR) -DSRC_INSTALL_DIR=$(THIRD_PARTY_SRC_DIR)

third-party-clean:
	rm -rf "$(THIRD_PARTY_BUILD_DIR)" "$(THIRD_PARTY_SRC_DIR)"

HYBRIDSE_BUILD_DIR := $(MAKEFILE_DIR)/hybridse/build
HYBRIDSE_INSTALL_DIR := $(THIRD_PARTY_DIR)/hybridse

hybridse: hybridse-build

hybridse-install: hybridse-build
	$(CMAKE_PRG) --build $(HYBRIDSE_BUILD_DIR) --target install

hybridse-test: hybridse-build
	$(CMAKE_PRG) --build $(HYBRIDSE_BUILD_DIR) --target test -- -j$(NPROC)

hybridse-build: hybridse-configure
	$(CMAKE_PRG) --build $(HYBRIDSE_BUILD_DIR) -- -j$(NPROC)

hybridse-configure: third-party
	$(CMAKE_PRG) -S hybridse -B $(HYBRIDSE_BUILD_DIR) -DCMAKE_PREFIX_PATH=$(THIRD_PARTY_DIR) -DCMAKE_INSTALL_PREFIX=$(HYBRIDSE_INSTALL_DIR) $(CMAKE_FLAGS)

hybridse-clean:
	rm -rf "$(HYBRIDSE_BUILD_DIR)"

clean: hybridse-clean openmldb-clean

distclean: clean third-party-clean

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
