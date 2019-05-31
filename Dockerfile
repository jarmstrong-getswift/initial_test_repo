FROM openjdk:8-jre

ARG talend_job=ARCTEAM402_Update_Last_Credit_Date_Salesforce
ARG talend_version=0.1

LABEL maintainer="jarmstrong@getswift.co" \
    talend.job=${talend_job} \
    talend.version=${talend_version}

ENV TALEND_JOB ${talend_job}
ENV TALEND_VERSION ${talend_version}
ENV ARGS ""

WORKDIR /opt/talend

COPY ${TALEND_JOB}_${TALEND_VERSION}.zip .

### Install Talend Job
RUN unzip ${TALEND_JOB}_${TALEND_VERSION}.zip && \
    rm -rf ${TALEND_JOB}_${TALEND_VERSION}.zip && \
    chmod +x ${TALEND_JOB}/${TALEND_JOB}_run.sh

VOLUME /data

CMD ["/bin/sh","-c","${TALEND_JOB}/${TALEND_JOB}_run.sh ${ARGS}"]
