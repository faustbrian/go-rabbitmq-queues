GOLIB ?= golib

.PHONY: check ci cohesion config inventory repository-check specification-check workflows

config:
	$(GOLIB) config validate

inventory:
	$(GOLIB) inventory

cohesion:
	$(GOLIB) cohesion check

repository-check:
	$(GOLIB) repository check

specification-check:
	$(GOLIB) specification check --online

workflows:
	$(GOLIB) workflows check

check:
	$(GOLIB) check --all

ci: config inventory cohesion repository-check specification-check workflows check
