FROM tkrzw

WORKDIR /usr/local/workspace/tkrzw-python
ADD ./ /usr/local/workspace/tkrzw-python/

RUN apt-get update && apt-get -y install python3-pip python3-dev python3-setuptools && pip3 install regex --break-system-packages && make && make install
