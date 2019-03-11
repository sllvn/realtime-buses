FROM ubuntu:18.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y python3-pip
RUN pip3 install awscli
RUN apt-get install -y pv jq zip sed p7zip-full p7zip-rar

COPY process_raw_json_locations.sh transform_json.sh /root/

WORKDIR /root
