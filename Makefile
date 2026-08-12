TEST_POSTGRES_DSN ?= postgres://postgres:test@localhost:5433/bitmagnet_test?sslmode=disable
PARITY_IMAGE ?=
TAPE_DIR ?=
OUTPUT_DIR ?=

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

# Run the two-language classifier tape gate from one provenance-labelled image.
# Example:
#   make classifier-tape-parity \
#     PARITY_IMAGE=bitmagnet-writeset-replay:p0-<sha> \
#     TAPE_DIR=/absolute/path/to/tape \
#     OUTPUT_DIR=/absolute/path/to/new-evidence-directory
.PHONY: classifier-tape-parity
classifier-tape-parity:
	@set -eu; \
	test -n "$(PARITY_IMAGE)" || { echo "PARITY_IMAGE is required" >&2; exit 2; }; \
	test -n "$(TAPE_DIR)" || { echo "TAPE_DIR is required" >&2; exit 2; }; \
	test -d "$(TAPE_DIR)" || { echo "TAPE_DIR must be an existing directory" >&2; exit 2; }; \
	test -n "$(OUTPUT_DIR)" || { echo "OUTPUT_DIR is required" >&2; exit 2; }; \
	test ! -e "$(OUTPUT_DIR)" || { echo "OUTPUT_DIR must not already exist" >&2; exit 2; }; \
	tape_dir="$$(cd "$(TAPE_DIR)" && pwd -P)"; \
	output_name="$$(basename "$(OUTPUT_DIR)")"; \
	output_parent="$$(cd "$$(dirname "$(OUTPUT_DIR)")" && pwd -P)"; \
	test -w "$$output_parent" || { echo "OUTPUT_DIR parent must be writable" >&2; exit 2; }; \
	docker run --rm --network none --read-only --cap-drop ALL \
		--security-opt no-new-privileges \
		--user "$$(id -u):$$(id -g)" \
		--volume "$$tape_dir:/input:ro" \
		--volume "$$output_parent:/evidence" \
		--entrypoint /usr/local/bin/bitmagnet \
		"$(PARITY_IMAGE)" classifier tape-parity \
			--dir /input \
			--output-dir "/evidence/$$output_name"
