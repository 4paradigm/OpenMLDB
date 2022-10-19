MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR  := $(dir $(MAKEFILE_PATH))

POETRY_PRG ?= $(shell (command -v poetry))
DOXYBOOK2_PRG ?= $(shell (command -v doxybook2 || echo doxybook2))

UDF_GEN_DIR ?= $(MAKEFILE_DIR)/udfgen

.PHONY: all doxygen doxygen2md clean

all: doxygen2md

doxygen:
	if [ -n "$(POETRY_PRG)" ]; then \
	    $(POETRY_PRG) run python export_udf_doc.py; \
	else \
	    ./export_udf_doc.py; \
	fi

doxygen2md: doxygen
	mkdir -p $(UDF_GEN_DIR)
	$(DOXYBOOK2_PRG) --input xml --output $(UDF_GEN_DIR) --config config.json --templates ../template --summary-input SUMMARY.md.tmpl --summary-output SUMMARY.md
	@echo "udf document write into $(MAKEFILE_DIR)/udfgen/Files/udfs_8h.md"

clean:
	rm -rf xml/ html/ udfs/ $(UDF_GEN_DIR)
