
build:
	mkdir ./dist
	cp -R input output main.py test.py ./dist
	cd ./dependencies && zip -r ../dist/dep.zip .