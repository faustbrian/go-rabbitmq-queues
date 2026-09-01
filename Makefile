GOLIB ?= golib

.PHONY: check ci cohesion config inventory repository-check workflows

config:
	$(GOLIB) config validate

inventory:
	$(GOLIB) inventory

cohesion:
	$(GOLIB) cohesion check

repository-check:
	$(GOLIB) repository check

workflows:
	$(GOLIB) workflows check

check:
	$(GOLIB) check --all

ci: config inventory cohesion repository-check workflows check
