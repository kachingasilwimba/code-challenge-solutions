# code-challenge-solutions

## Setup
pip install -r requirements.txt

## Initialize + Ingest
python -m src.ingest --data-dir ./wx_data --db weather.db

## confirm uniqueness in SQL
sqlite3 weather.db "SELECT COUNT(*) AS n FROM weather_observation;"
sqlite3 weather.db "SELECT COUNT(*) AS dups FROM (SELECT station_id, obs_date, COUNT(*) c FROM weather_observation GROUP BY station_id, obs_date HAVING c>1);"

## Compute yearly stats
python -m src.calc_stats --db weather.db

## Run API
uvicorn src.api:app --reload

## API Docs
http://127.0.0.1:8000/docs

## Run tests
pytest -q
