#FROM python:3.11.0-buster
FROM rocker/r-ver:4.2.1

######################################################################################
# Install Linux packages
######################################################################################

#RUN apt-get update && apt-get install -y --no-install-recommends --allow-unauthenticated \
#      build-essential wget curl git gawk time

######################################################################################
# Install Python3 and modules.
######################################################################################

RUN apt update && apt upgrade -y
RUN apt install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt update \
 && apt install python3.11 -y

#RUN pip3 install "pandas==1.5.1"

######################################################################################
# Install R
######################################################################################

# update indices
#RUN apt update -qq
## install two helper packages we need
#RUN apt install --no-install-recommends software-properties-common dirmngr -y
## add the signing key (by Michael Rutter) for these repos
## To verify key, run gpg --show-keys /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc 
## Fingerprint: E298A3A825C0D65DFD57CBB651716619E084DAB9
#RUN wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc
## add the R 4.0 repo from CRAN -- adjust 'focal' to 'groovy' or 'bionic' as needed
#RUN add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"
#RUN apt install --no-install-recommends r-base -y
#
##RUN apt install ca-certificates -y
#RUN wget https://download.docker.com/linux/ubuntu/gpg \
# && apt-key add gpg
#RUN add-apt-repository -y ppa:c2d4u.team/c2d4u4.0+

#ENV TZ=America/Denver

#RUN apt-get update && apt-get install -y apt-transport-https software-properties-common && \
#    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
#    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' && \
#    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
#    apt update && \
#    apt install -y r-base r-base-dev libcurl4-openssl-dev libssl-dev libxml2-dev && \
#    R -e "install.packages(c('data.table', 'feather', 'fst', 'readr', 'dplyr'), repos='https://rweb.crmda.ku.edu/cran/')"

######################################################################################
# Install zstandard
######################################################################################

#ENV zstd_ver=1.4.4

#RUN mkdir /zstd && \
#    cd /zstd && \
#    wget -O zstd-${zstd_ver}.tar.gz \
#      https://github.com/facebook/zstd/releases/download/v${zstd_ver}/zstd-${zstd_ver}.tar.gz && \
#    tar -zxvf zstd-${zstd_ver}.tar.gz && \
#    rm zstd-${zstd_ver}.tar.gz && \
#    mv zstd-${zstd_ver}/* . && \
#    rm -rf zstd-${zstd_ver} && \
#    make install

######################################################################################
# Add and compile C++ code
######################################################################################

#COPY *.cpp /

#RUN g++ -O3 -std=gnu++14 -o /TestFixedWidth2 TestFixedWidth2.cpp && \
#    chmod +x /TestFixedWidth2 && \
#    g++ -O3 -std=gnu++14 -o /TestFixedWidth3 TestFixedWidth3.cpp && \
#    chmod +x /TestFixedWidth3
#RUN g++ -O3 -std=gnu++14 TestFixedWidth4T.cpp -lz -lzstd -o /TestFixedWidth4T && \
#    chmod +x /TestFixedWidth4T

######################################################################################
# Install Rust and compile Rust code
######################################################################################

#RUN curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh -s -- -y
#RUN mkdir -p /Rust/TestFixedWidth2/src /Rust/TestFixedWidth3/src /Rust/TestFixedWidth4/src /Rust/TestFixedWidth4T/src

#COPY Rust/Cargo.toml /Rust/TestFixedWidth2/Cargo.toml
#COPY Rust/TestFixedWidth2.rs /Rust/TestFixedWidth2/src/main.rs
#RUN cd /Rust/TestFixedWidth2; /root/.cargo/bin/cargo build --release

#COPY Rust/Cargo.toml /Rust/TestFixedWidth3/Cargo.toml
#COPY Rust/TestFixedWidth3.rs /Rust/TestFixedWidth3/src/main.rs
#RUN cd /Rust/TestFixedWidth3; /root/.cargo/bin/cargo build --release

#COPY Rust/Cargo.toml /Rust/TestFixedWidth4/Cargo.toml
#COPY Rust/TestFixedWidth4.rs /Rust/TestFixedWidth4/src/main.rs
#RUN cd /Rust/TestFixedWidth4; /root/.cargo/bin/cargo build --release

#COPY Rust/Cargo.toml /Rust/TestFixedWidth4T/Cargo.toml
#COPY Rust/TestFixedWidth4T.rs /Rust/TestFixedWidth4T/src/main.rs
#RUN cd /Rust/TestFixedWidth4T; /root/.cargo/bin/cargo build --release

#ADD F4/f4py /f4py/
##RUN git clone https://github.com/srp33/F4 \
## && mv F4/f4py /

######################################################################################
# Add scripts that we created
######################################################################################

#COPY *.R /
#COPY *.py /
#COPY test.sh /
