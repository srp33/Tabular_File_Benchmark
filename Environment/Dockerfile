FROM ubuntu:18.04

######################################################################################
# Install Linux packages
######################################################################################

RUN apt-get update && apt-get install -y --no-install-recommends --allow-unauthenticated \
      build-essential python3-dev python3-setuptools python3-pip libsnappy-dev \
      gawk time wget curl \
 && pip3 install --upgrade setuptools pip

######################################################################################
# Install Python3 and modules.
######################################################################################

RUN pip3 install "msgpack==0.5.6" "numpy==1.15.2" "pandas==0.23.4" "python-snappy==0.5.3" \
                 "tables==3.4.4" "zstandard==0.10.2" "lz4==2.1.6" "fastnumbers==2.1.1" \
                 "joblib==0.15.1"

######################################################################################
# Install R
######################################################################################

ENV TZ=America/Denver

RUN apt-get update && apt-get install -y apt-transport-https software-properties-common && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    apt update && \
    apt install -y r-base r-base-dev libcurl4-openssl-dev libssl-dev libxml2-dev && \
    R -e "install.packages(c('data.table', 'feather', 'fst', 'readr', 'dplyr'), repos='https://rweb.crmda.ku.edu/cran/')"

######################################################################################
# Install zstandard
######################################################################################

ENV zstd_ver=1.4.4

RUN mkdir /zstd && \
    cd /zstd && \
    wget -O zstd-${zstd_ver}.tar.gz \
      https://github.com/facebook/zstd/releases/download/v${zstd_ver}/zstd-${zstd_ver}.tar.gz && \
    tar -zxvf zstd-${zstd_ver}.tar.gz && \
    rm zstd-${zstd_ver}.tar.gz && \
    mv zstd-${zstd_ver}/* . && \
    rm -rf zstd-${zstd_ver} && \
    make install

######################################################################################
# Add and compile C++ code
######################################################################################

COPY *.cpp /

RUN g++ -O3 -std=gnu++14 -o /TestFixedWidth2 TestFixedWidth2.cpp && \
    chmod +x /TestFixedWidth2 && \
    g++ -O3 -std=gnu++14 -o /TestFixedWidth3 TestFixedWidth3.cpp && \
    chmod +x /TestFixedWidth3
RUN g++ -O3 -std=gnu++14 TestFixedWidth4T.cpp -lz -lzstd -o /TestFixedWidth4T && \
    chmod +x /TestFixedWidth4T

######################################################################################
# Install Rust and compile Rust code
######################################################################################

RUN curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh -s -- -y
RUN mkdir -p /Rust/TestFixedWidth2/src /Rust/TestFixedWidth3/src /Rust/TestFixedWidth4/src /Rust/TestFixedWidth4T/src

COPY Rust/Cargo.toml /Rust/TestFixedWidth2/Cargo.toml
COPY Rust/TestFixedWidth2.rs /Rust/TestFixedWidth2/src/main.rs
RUN cd /Rust/TestFixedWidth2; /root/.cargo/bin/cargo build --release

COPY Rust/Cargo.toml /Rust/TestFixedWidth3/Cargo.toml
COPY Rust/TestFixedWidth3.rs /Rust/TestFixedWidth3/src/main.rs
RUN cd /Rust/TestFixedWidth3; /root/.cargo/bin/cargo build --release

COPY Rust/Cargo.toml /Rust/TestFixedWidth4/Cargo.toml
COPY Rust/TestFixedWidth4.rs /Rust/TestFixedWidth4/src/main.rs
RUN cd /Rust/TestFixedWidth4; /root/.cargo/bin/cargo build --release

COPY Rust/Cargo.toml /Rust/TestFixedWidth4T/Cargo.toml
COPY Rust/TestFixedWidth4T.rs /Rust/TestFixedWidth4T/src/main.rs
RUN cd /Rust/TestFixedWidth4T; /root/.cargo/bin/cargo build --release

######################################################################################
# Add scripts that we created
######################################################################################

COPY *.py /
COPY *.R /
COPY test.sh /
