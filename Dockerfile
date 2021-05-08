FROM hub.mingdao.com/md/centos7:imagemagick-7.0.9_ffmpeg-3.4.2

LABEL maintainer="Mingdao Inc <wells>"

ENV GOPATH /go
ENV CGO_ENABLED 0
ENV GO111MODULE on

RUN mkdir -p /usr/local/file
RUN mkdir -p /data/storage
RUN mkdir -p /data/storage/data
RUN mkdir -p /data/storage/tmp
RUN mkdir -p /data/storage/multitmp
RUN mkdir -p /data/storage/fetchtmp
RUN mkdir -p /data/storage/cache


WORKDIR /usr/local/file


ENV TIME_ZONE Asia/Shanghai
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


COPY  font /usr/local/file/font
COPY  main /usr/local/file/


EXPOSE 9000



CMD ["./main"]
