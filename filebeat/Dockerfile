ARG ELK_VERSION

FROM docker.elastic.co/beats/filebeat:$ELK_VERSION
RUN mkdir /usr/share/filebeat/logs/tank
COPY conf/filebeat.yml /usr/share/filebeat/filebeat.yml
USER root
RUN chmod go-w /usr/share/filebeat/filebeat.yml
USER filebeat