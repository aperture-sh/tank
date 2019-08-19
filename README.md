# Tank
[![Apache License, Version 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0) [![Build Status](https://travis-ci.org/marauder-io/tank.svg?branch=master)](https://travis-ci.org/marauder-io/tank) ![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/maraud3r/tank)

## Visualization Focused Database

The tank stores geospatial data in a distributed way. The main endpoint provides 

`application.kt` contains the main module to start the server application.

### Prerequisites
Run a Cassandra instance:  
`docker run --name cassandra -d  -p 9042:9042 maraud3r/cassandra-lucene:latest`  
Connecting to DB is possible using the cqshl:  
`docker run -it --link cassandra --rm cassandra sh -c 'exec cqlsh "$CASSANDRA_PORT_9042_TCP_ADDR"'`

### Quick Start

The `TANK_DB_HOSTS` variable is a comma-separated host list  
`docker run --name tank -d -p 8888:8888 -e TANK_DB_HOSTS=cassandra --link cassandra tank`

### REST API

`GET /` - Print Tank info message  
`POST /:layer?` accepts GeoJSON features separated by line to import, `geojson=true` indicates to import a GeoJSON file, the layer parameter is not used at the current release  
`GET /tile/z/x/y` - Request a tile, put `filter={"main_attr": "test"}` to filter the main attribute  
`GET /static/index.html` - Simple mapping interface for data exploration

### Configuration

The configuration can be done using the `application.conf` HOCON file.  

Most important are all setting regarding database keys and attributes to be stored.
The `tyler` section describes which attributes are put into the served tile and which attribute is used for filtering upon request.
The `data` section describes which attributes are stored using which type.
If `add_timestamp` is set to `true`, a timestamp field has to be set (as it is by default). Then the database will store the current time and date in that particular field.

Adding `-config=resources/application.conf` can be used to specify another configuration file.

#### Hints

* For now there are no additional indices build for doing queries on other field than geometry and the partition keys.
* `geometry` is a reserved field. One is not allowed to use it as a attribute field in the configuration.
* `timestamp` is a reserved field. One is not allowed to use it for other purposes than storing the current timestamp.
* `hash` and `uid` are reserved fields to. These are the mandatory partition key fields

License
-------

Tank is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
