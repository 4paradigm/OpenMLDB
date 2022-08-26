.PHONY: all en zh clean init

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR  := $(dir $(MAKEFILE_PATH))

POETRY_PRG ?= $(shell (command -v poetry || echo poetry))
OUT_DIR ?= $(MAKEFILE_DIR)/build


all: en zh

en: init
	$(POETRY_PRG) run sphinx-multiversion "$(MAKEFILE_DIR)/en" "$(OUT_DIR)/en"

zh: init
	$(POETRY_PRG) run sphinx-multiversion "$(MAKEFILE_DIR)/zh" "$(OUT_DIR)/zh"

init:
	$(POETRY_PRG) install

clean:
	rm -rvf "$(OUT_DIR)"
