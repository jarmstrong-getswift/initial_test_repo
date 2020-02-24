FROM openjdk:7-jdk

USER root
#comment in if desiring to keep the container up after build and perform docker run with -d flag
CMD tail -f /dev/null

#ENV statements will potentially be overwritten at runtime -- runtime syntax follows
#ENV values are substituted like ksh values, i.e., ${env_variable_name}
ENV LOCATIONS=/staging
ENV CONTACTPOINTS=192.168.150.41
ENV USERNAME=vagrant
ENV PASSWORD=vagrant
ENV KEYSPACE_NAME=cash_management

RUN apt-get update
RUN apt install -y maven
RUN mkdir -p /cassandra_migration
COPY cassandra-migration-master.zip /cassandra_migration
RUN cd /cassandra_migration; unzip cassandra-migration-master.zip
RUN cd /cassandra_migration/cassandra-migration-master; mvn compile 
RUN cd /cassandra_migration/cassandra-migration-master; mvn test
RUN cd /cassandra_migration/cassandra-migration-master; mvn package
RUN cd /cassandra_migration/cassandra-migration-master; mvn install

CMD cd /cassandra_migration/cassandra-migration-master; java -jar -Dcassandra.migration.scripts.locations=filesystem:${LOCATIONS} -Dcassandra.migration.cluster.contactpoints=${CONTACTPOINTS} -Dcassandra.migration.cluster.username=${USERNAME} -Dcassandra.migration.cluster.password=${PASSWORD} -Dcassandra.migration.keyspace.name=${KEYSPACE_NAME} target/*-jar-with-dependencies.jar migrate
