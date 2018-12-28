#!/bin/bash

#htslib
VERSIONH="1.9"
NAMEH="htslib"
URLH="https://github.com/samtools/${NAMEH}/releases/download/${VERSIONH}/${NAMEH}-${VERSIONH}.tar.bz2"

#samtools
VERSIONS="1.9"
NAMES="samtools"
URLS="https://github.com/samtools/${NAMES}/releases/download/${VERSIONS}/${NAMES}-${VERSIONS}.tar.bz2"

#Update the vm
sudo apt-get update
sudo apt-get -y install \
build-essential \
zlib1g-dev \
libncurses5-dev \
libbz2-dev \
liblzma-dev \
libcurl4-openssl-dev \
libssl-dev

#build htslib
mkdir htslib
wget ${URLH}
tar xvjf ${NAMEH}-${VERSIONH}.tar.bz2
cd ${NAMEH}-${VERSIONH}
make
sudo make install
cd ..

#build samtools
wget ${URLS}
tar xvjf ${NAMES}-${VERSIONS}.tar.bz2
cd ${NAMES}-${VERSIONS}
make
sudo make install
cd ..
