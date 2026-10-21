.PHONY: demo test generate mirror docker

generate:        ## write CDC events to ./cdc_out
	python src/generator/generate_cdc.py --out cdc_out

mirror:          ## one-shot merge into ./delta/orders
	spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
	  src/streaming/mirror_job.py --input cdc_out --delta delta/orders --once

demo: generate mirror  ## full local demo

test:            ## pure-python unit tests (no Spark needed)
	python -m pytest tests/ -q

docker:          ## demo inside Docker
	docker build -t sap-cdc-order-mirror .
	docker run --rm sap-cdc-order-mirror
