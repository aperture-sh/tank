FROM openjdk:11-jdk AS build

ADD . /build
WORKDIR /build

RUN ./gradlew installDist

FROM openjdk:11-jre

ENV APPLICATION_USER ktor
RUN useradd -s /bin/bash -m $APPLICATION_USER

RUN mkdir -p /app/logs
RUN mkdir /storage
RUN chown -R $APPLICATION_USER /app
RUN chown -R $APPLICATION_USER /storage

USER $APPLICATION_USER

EXPOSE 8888/tcp

COPY --from=build /build/build/install/tank /app
COPY --from=build /build/resources/application.conf /app/application.conf
WORKDIR /app

ENTRYPOINT ["/app/bin/tank"]

CMD ["-config=/app/application.conf"]