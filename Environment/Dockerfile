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
COPY Rust/Cargo.toml /
COPY Rust/TestFixedWidth2.rs /src/main.rs
RUN /root/.cargo/bin/cargo build
#RUN /root/.cargo/bin/rustc -o /TestFixedWidth2_rust /TestFixedWidth2.rs

######################################################################################
# Add scripts that we created
######################################################################################

COPY *.py /
COPY *.R /
COPY test.sh /

#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/html' -exec rm -r "{}" \; \
#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/data' -exec rm -r "{}" \; \
#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/doc' -exec rm -r "{}" \; \
#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/tests' -exec rm -r "{}" \; \
#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/examples' -exec rm -r "{}" \; \
#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/help' -exec rm -r "{}" \; \
#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/www' -exec rm -r "{}" \; \
#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/www-dir' -exec rm -r "{}" \; \
#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/staticdocs' -exec rm -r "{}" \; \
#  && find /usr/local/lib/R/site-library/ -depth -wholename '*/demo' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/html' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/data' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/doc' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/tests' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/examples' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/help' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/www' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/www-dir' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/staticdocs' -exec rm -r "{}" \; \
#  && find /usr/lib/R/library/ -depth -wholename '*/demo' -exec rm -r "{}" \; \
#  && rm -rf /usr/local/lib/R/site-library/BH \
#  && rm -rf /usr/share/mime /usr/share/tcltk  \
#  && rm -rf /usr/share/tcltk /usr/share/man \
#  && rm -rf /usr/share/doc /usr/share/locale /usr/share/perl5 \
#  && apt-get -y autoremove \
#  && apt-get -y clean
