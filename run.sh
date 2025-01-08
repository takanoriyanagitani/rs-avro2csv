#!/bin/sh

export ENV_SCHEMA_FILENAME=./sample.d/input.avsc

jsons2avro() {
	cat sample.d/sample.jsonl |
		json2avrows |
		cat >./sample.d/input.avro
}

#jsons2avro

run_native(){
	cat ./sample.d/input.avro |
		./rs-avro2csv |
		bat --language=csv
}

run_wazero(){
	cat ./sample.d/input.avro |
		wazero run ./rs-avro2csv.wasm |
		bat --language=csv
}

run_wasmtime(){
	cat ./sample.d/input.avro |
		wasmtime run ./rs-avro2csv.wasm |
		bat --language=csv
}

run_wasmer(){
	cat ./sample.d/input.avro |
		wasmer run ./rs-avro2csv.wasm |
		bat --language=csv
}

run_native
#run_wasmtime
#run_wasmer
#run_wazero
