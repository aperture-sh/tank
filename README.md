## Prototype of Visualization Focused Database

`application.kt` contains the main module to start the server application.

### Prerequisites
Connect using the cqshl:  
`docker run -it --link some-cassandra:cassandra --rm cassandra sh -c 'exec cqlsh "$CASSANDRA_PORT_9042_TCP_ADDR"'`

Run:
```sql
CREATE  KEYSPACE geo
   WITH REPLICATION = { 
      'class' : 'SimpleStrategy', 'replication_factor' : N };
USE geo;
CREATE TABLE features (z int, x int, y int, id text, geometry blob, PRIMARY KEY (z, x, y, id));
```


### Quick Start

`docker run --name tank -d -p 8888:8888 -e TANK_DB_HOST=cassandra --link cassandra tank`

### REST API

`GET /` - Print Tank info message  
`POST /` - Import GeoJSON features line separated  
`POST /file` -  Import GeoJSON feature collection as one file  
`GET /z/x/y` - Request a complete tile