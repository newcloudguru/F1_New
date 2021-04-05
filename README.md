__author__ = Fahim Safedien

# Running the application

`create a new virtual environment`

`virtualenv venv`

`source venv/bin/activate`

`pip install -r requirements.txt`

`make build`

`spark-submit --master local test.py`
`spark-submit --master local main.py`

`cd dist && spark-submit --master local --py-files dep.zip main.py`

