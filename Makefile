build:
	mkdir -p ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x entrypoint.py -r ../dist/jobs.zip .
