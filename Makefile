.PHONY: setup
setup:
	@sh ./.scripts/setup.sh

.PHONY: tidy
tidy:
	@go mod tidy

.PHONY: ut
ut:
	@go test -race ./...

.PHONY: fmt
fmt:
	@sh ./.scripts/fmt.sh

.PHONY: check
check:
	@$(MAKE) --no-print-directory setup
	@$(MAKE) --no-print-directory tidy
	@$(MAKE) --no-print-directory fmt
	@$(MAKE) --no-print-directory ut
