# TMDB Import

Django backend keeping track of all data and updates from TMDB
This contains the base data that gets propagated to all other systems.

As soon as a movie gets fetched/updated/deleted, an event will be sent to kafka
containing the movie-id, and the event-type. Respective systems will decide if they
want to do something with this info, and fetch the data through http-requests

### Mongo restore
```bash
# The fashion way
docker exec -ti mongo mongodump --out=/mongodump && docker cp mongo:/mongodump .

docker cp mongodump mongo:/ && docker exec -ti mongo mongorestore /mongodump

# Kubernetes way
kubectl cp ~/workspace/personal/worldinmovie/mongodump mongo-69776f664b-vctdb:/
kubectl exec -ti mongo-69776f664b-vctdb mongorestore /mongodump
kubectl exec -ti mongo-69776f664b-vctdb /bin/bash

# The boomer way
docker exec -ti mongo mongoexport -d tmdb -c movie  --out datadump.json
docker cp mongo:/datadump.json .
docker cp datadump.json mongo:/ 
docker exec -ti mongo mongoimport -d tmdb -c movie --mode upsert --file datadump.json
```


```bash
python -m venv venv

source venv/bin/activate
deactivate

pip install -r requirements.txt

DOCKER_HOST=unix:///Users/daveri/.docker/run/docker.sock ./manage.py behave

MONGO_URL=localhost:27017 gunicorn --config=gunicorn.config.py -k uvicorn.workers.UvicornWorker --reload settings.asgi

docker buildx build --platform linux/amd64,linux/arm64 -t seppaleinen/worldinmovies_tmdb:latest .
```

### Mongo Debugging
```
db.setProfilingLevel(2)

db.system.profile.find().pretty()
```