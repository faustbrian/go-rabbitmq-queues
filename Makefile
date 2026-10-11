GOLIB ?= golib

.PHONY: check check-full ci cohesion config inventory repository-check specification-check specification-check-online workflows

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

check:
	$(GOLIB) check --local --all

check-full:
	$(GOLIB) check --all

ci: config inventory cohesion repository-check specification-check workflows check
