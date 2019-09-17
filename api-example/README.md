
# API Example
An example API with 2 registries.  One is the Druid Wikipedia example, the second is a contrived example with sample data in H2.  For the Druid example, you'd have to follow the Druid tutorial so you have a locally running Druid cluster with that data.

Example endpoints:

```
GET /registry/wiki/domain
POST /registry/wiki/schemas/wiki/query
GET /registry/student/domain
POST /registry/student/schemas/student/query
```

# Run api example with docker

```
mvn clean install
docker build -t maha-api-example .
docker run -d -p8080:8080 maha-api-example
```
