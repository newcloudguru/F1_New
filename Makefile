build:
	mkdir ./dist 
	mkdir ./dist/output
	mkdir ./dist/output/avg_laptimes
	mkdir ./dist/output/avg_laptimes_salary
	cp input -r main.py ./dist
	cd src && zip -r ../dist/src.zip utilities
