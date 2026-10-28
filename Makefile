RUN := docker compose --progress quiet run --rm $(if $(SEED),-e SEED=$(SEED) )pipeline

.PHONY: demo reset load mutate dedup check break restore test

# Each step must exit with the expected code. Steps expected to fail call the container directly,
# because make turns any failing recipe into exit code 2.
demo:
	@echo "== 1. reset ==" && $(MAKE) --no-print-directory reset
	@echo "== 2. load (initial, full) ==" && $(MAKE) --no-print-directory load
	@echo "== 2. check ==" && $(MAKE) --no-print-directory check
	@echo "== 3. dedup (before mutate) ==" && $(MAKE) --no-print-directory dedup
	@echo "== 4. mutate ==" && $(MAKE) --no-print-directory mutate
	@echo "== 5. load FAIL_AT=customers (expected exit 1) =="
	$(RUN) python -m pipeline.load --fail-at customers; test $$? -eq 1
	@echo "== 6. load (recovery; the measured incremental load) ==" && $(MAKE) --no-print-directory load
	@echo "== 7. load (expected 0 changed) ==" && $(MAKE) --no-print-directory load
	@echo "== 8. dedup (recreated duplicate becomes merged) ==" && $(MAKE) --no-print-directory dedup
	@echo "== 8. check ==" && $(MAKE) --no-print-directory check
	@echo "== 9. break ==" && $(MAKE) --no-print-directory break
	@echo "== 9. check (expected exit 1, four FAIL) =="
	$(RUN) python -m pipeline.check; test $$? -eq 1
	@echo "== 9. restore ==" && $(MAKE) --no-print-directory restore
	@echo "== 9. check ==" && $(MAKE) --no-print-directory check
	@echo "== 10. test ==" && $(MAKE) --no-print-directory test
	@echo "== demo: every step returned the expected exit code =="

reset:
	$(RUN) sh -c "python -m seed.seed && rm -rf data"

load:
	$(RUN) python -m pipeline.load $(if $(FAIL_AT),--fail-at $(FAIL_AT))

mutate:
	$(RUN) python -m pipeline.mutate

dedup:
	$(RUN) python -m pipeline.dedup

check:
	$(RUN) python -m pipeline.check

break:
	$(RUN) python -m pipeline.break_restore break

restore:
	$(RUN) python -m pipeline.break_restore restore

test:
	$(RUN) python -m pytest -q tests
