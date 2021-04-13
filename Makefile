
build:
	mkdir ./dist 
	mkdir ./dist/output
	cp input -r main.py ./dist
	cd src && zip -r ../dist/src.zip utilities
