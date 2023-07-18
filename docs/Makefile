.PHONY: all en zh en-local zh-local clean init all-local

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR  := $(dir $(MAKEFILE_PATH))

POETRY_PRG ?= $(shell (command -v poetry || echo poetry))
OUT_DIR ?= $(MAKEFILE_DIR)/build


all: en zh
	echo '<!DOCTYPE html> <html><head><meta http-equiv="refresh" content="0; url=zh" /></head></html>' > $(OUT_DIR)/index.html

en: init
	$(POETRY_PRG) run sphinx-multiversion "$(MAKEFILE_DIR)/en" "$(OUT_DIR)/en"
	echo '<!DOCTYPE html> <html><head><meta http-equiv="refresh" content="0; url=main" /></head></html>' > $(OUT_DIR)/en/index.html

zh: init
	$(POETRY_PRG) run sphinx-multiversion "$(MAKEFILE_DIR)/zh" "$(OUT_DIR)/zh"
	echo '<!DOCTYPE html> <html><head><meta http-equiv="refresh" content="0; url=main" /></head></html>' > $(OUT_DIR)/zh/index.html

# for local build, you may need compile current branch only, use the three jobs
all-local: en-local zh-local

en-local: init
	$(POETRY_PRG) run sphinx-build "$(MAKEFILE_DIR)/en" "$(OUT_DIR)/en-local"

zh-local: init
	$(POETRY_PRG) run sphinx-build "$(MAKEFILE_DIR)/zh" "$(OUT_DIR)/zh-local"

init:
	$(POETRY_PRG) install

clean:
	rm -rvf "$(OUT_DIR)"
