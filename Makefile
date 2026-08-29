GOLIB ?= golib

.PHONY: check ci config inventory repository-check workflows

config:
	$(GOLIB) config validate

inventory:
	$(GOLIB) inventory

repository-check:
	$(GOLIB) repository check

workflows:
	$(GOLIB) workflows check

check:
	$(GOLIB) check --all

ci: config inventory repository-check workflows check
