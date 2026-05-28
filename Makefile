TEST_POSTGRES_DSN ?= postgres://postgres:test@localhost:5433/bitmagnet_test?sslmode=disable

.PHONY: test
test:
	go test -count=1 ./...

.PHONY: test-e2e
test-e2e:
	docker compose -f docker-compose.test.yml up -d --wait
	POSTGRES_DSN="$(TEST_POSTGRES_DSN)" go test -tags integration -v -count=1 ./internal/blobmigration/...
	docker compose -f docker-compose.test.yml down

.PHONY: test-e2e-down
test-e2e-down:
	docker compose -f docker-compose.test.yml down
