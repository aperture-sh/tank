## Prototype of Visualization Focused Database

`application.kt` contains the main module to start the server application.

### Prerequisites
Run a Cassandra instance:  
`docker run --name cassandra -d  -p 9042:9042 cassandra:latest`  
Connect using the cqshl:  
`docker run -it --link cassandra --rm cassandra sh -c 'exec cqlsh "$CASSANDRA_PORT_9042_TCP_ADDR"'`

Replace `N` with choosen replication factor, e.g. `1` and run:
```sql
CREATE  KEYSPACE geo
   WITH REPLICATION = { 
      'class' : 'SimpleStrategy', 'replication_factor' : N };
USE geo;
CREATE TABLE features (z int, x int, y int, id text, geometry blob, PRIMARY KEY (z, x, y, id));
```

```sql
CREATE TABLE geo.features (
    timestamp timestamp,
    id text,
    geometry text,
    PRIMARY KEY (timestamp, id)
)
CREATE CUSTOM INDEX test_idx ON geo.features (geometry) USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {'refresh_seconds': '1', 'schema': '{       fields: { geometry: {             type: "geo_shape",             max_levels: 15          }       }    }'};
```


### Quick Start

The `TANK_DB_HOSTS` variable is a comma-separated host list  
`docker run --name tank -d -p 8888:8888 -e TANK_DB_HOSTS=cassandra --link cassandra tank`

### REST API

`GET /` - Print Tank info message  
`POST /` - Import GeoJSON features line separated  
`POST /file` -  Import GeoJSON feature collection as one file  
`GET /z/x/y` - Request a complete tile