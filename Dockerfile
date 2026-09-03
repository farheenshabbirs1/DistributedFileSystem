# Single image, two roles: infra/docker-compose.yml runs this image once per storage node
# (command: NodeServerMain) and once as the controller (command: ControllerMain) -- see
# infra/docker-compose.yml and README.md's "Running the containerized cluster" section.

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
# -DskipTests: the real test suite (mvn test) belongs in CI (.github/workflows/ci.yml), which
# has network access to resolve Maven Central; this stage just needs a jar to run.
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /build/target/distributed-file-system-1.0.0-shaded.jar app.jar

# No default CMD/ENTRYPOINT main class -- infra/docker-compose.yml sets `command` per service
# to select NodeServerMain or ControllerMain, since one image serves both roles.
