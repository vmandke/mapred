# mapred

### Code for medium blog. A fully functioning POC for MAP Reduce

Start the Driver, and Worker
```
PYTHONPATH=$MAPRED_ROOT python $MAPRED_ROOT/mapred/runner.py driver --port 5001
PYTHONPATH=$MAPRED_ROOT python $MAPRED_ROOT/mapred/runner.py worker --driver-uri http://127.0.0.1:5001 --port 5005
PYTHONPATH=$MAPRED_ROOT python $MAPRED_ROOT/mapred/runner.py worker --driver-uri http://127.0.0.1:5001 --port 5006
PYTHONPATH=$MAPRED_ROOT python $MAPRED_ROOT/mapred/runner.py worker --driver-uri http://127.0.0.1:5001 --port 5007
```

Read More here: https://medium.com/@vmandke/the-map-reduce-blueprint-58fbe9ddb1a4
