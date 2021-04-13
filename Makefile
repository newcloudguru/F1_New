
build:
	mkdir ./dist
	cp main.py ./dist
	cd ./src && zip -r ../dist/src.zip .

build2:
	mkdir ./dist
	cp input -r output main.py ./dist
	cd src && zip -r ../dist/src.zip utilities