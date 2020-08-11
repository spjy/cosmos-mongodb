FROM ubuntu:19.04 AS setup

# Install apt packages
RUN apt-get update
# Utility packages and Agent Mongo dependencies
RUN apt-get install curl build-essential wget libz-dev gcc-7 g++-7 cmake git openssl libssl-dev libsasl2-dev libboost-system-dev libboost-filesystem-dev libboost-chrono-dev libboost-program-options-dev libboost-test-dev -y
# Node, for COSMOS Web

FROM setup AS cosmos

# Run COSMOS quick installer
RUN git clone https://bitbucket.org/cosmos-project/installer.git /root/cosmos
RUN chmod +x /root/cosmos/cosmos-install.sh

WORKDIR /root/cosmos/
RUN /root/cosmos/cosmos-install.sh

WORKDIR /

FROM cosmos AS drivers

# Retrieve required repositories
RUN wget https://github.com/mongodb/mongo-c-driver/releases/download/1.17.0/mongo-c-driver-1.17.0.tar.gz \
  && tar xzf mongo-c-driver-1.17.0.tar.gz
RUN wget https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.6.0/mongo-cxx-driver-r3.6.0.tar.gz \
  && tar xzf mongo-cxx-driver-r3.6.0.tar.gz

# Mongo C Installation
WORKDIR /mongo-c-driver-1.17.0/cmake-build
RUN cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF .. \
  && make -j4 \
  && make install

# Mongo CXX Installation
WORKDIR /mongo-cxx-driver-r3.6.0.tar.gz/build
RUN cmake -DCMAKE_BUILD_TYPE=Release -DBSONCXX_POLY_USE_BOOST=1 -DCMAKE_INSTALL_PREFIX=/usr/local .. \
  && make -j4 \
  && make install

FROM drivers AS agentmongo

# Agent Mongo Installation
COPY . /root/cosmos/source/tools/mongodb
WORKDIR /root/cosmos/source/tools/mongodb/agent_build
RUN cmake ../source \
  && make -j4
