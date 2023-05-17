build:
	mkdir -p ./dist
	cp ./src/entrypoint.py ./dist
	cd ./src && zip -x entrypoint.py -r ../dist/jobs.zip .
