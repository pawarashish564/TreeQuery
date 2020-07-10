FROM openjdk:15-jdk-alpine

USER root
ENV APPUSER javaapp
ENV HOMEDIR /opt/${APPUSER}
ENV APPDIR /opt/app
ENV CONFIGDIR /opt/config
ENV DATADIR /opt/data
RUN mkdir -p ${HOMEDIR}
RUN mkdir -p ${CONFIGDIR}
RUN mkdir -p ${DATADIR}

ENV LD_LIBRARY_PATH /lib64
RUN apk add --update libc6-compat
ENV SOFTWARE_PATH build/distributions/
RUN addgroup -S $APPUSER && adduser -D -S -H -G $APPUSER -h $HOMEDIR $APPUSER
WORKDIR $APPDIR

ADD ${SOFTWARE_PATH}/TreeQueryNodeServerExecutable-1.0-SNAPSHOT.tar $APPDIR
COPY ./StartTreeQueryNode.sh $APPDIR
RUN chown -R $APPUSER:$APPUSER $HOMEDIR
RUN chgrp -R $APPUSER $APPDIR
RUN chmod 750 StartTreeQueryNode.sh
USER $APPUSER

CMD ["./StartTreeQueryNode.sh"]