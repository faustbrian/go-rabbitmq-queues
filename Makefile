GOLIB ?= golib

.PHONY: source-docs-check check check-full ci cohesion config inventory repository-check specification-check specification-check-online workflows

config:
	$(GOLIB) config validate

inventory:
	$(GOLIB) inventory

cohesion:
	$(GOLIB) cohesion check

repository-check:
	$(GOLIB) repository check

specification-check:
	$(GOLIB) specification check

specification-check-online:
	$(GOLIB) specification check --online

workflows:
	$(GOLIB) workflows check

check: source-docs-check
	$(GOLIB) check --local --all

check-full: source-docs-check
	$(GOLIB) check --all

ci: source-docs-check config inventory cohesion repository-check specification-check workflows check

source-docs-check:
	$(GOLIB) source-docs check
