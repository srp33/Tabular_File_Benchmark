FROM ubuntu:18.04

######################################################################################
# Install Linux packages
######################################################################################

RUN apt-get update && apt-get install -y --no-install-recommends --allow-unauthenticated \
      build-essential python3-dev python3-setuptools python3-pip libsnappy-dev \
      gawk time \
 && pip3 install --upgrade setuptools pip

######################################################################################
# Install Python3 and modules.
######################################################################################

RUN pip3 install "msgpack==0.5.6" "numpy==1.15.2" "pandas==0.23.4" "python-snappy==0.5.3" "tables==3.4.4" "zstandard==0.10.2" "lz4==2.1.6" "fastnumbers==2.1.1"

######################################################################################
# Install R
######################################################################################

ENV TZ=America/Denver

RUN apt-get update && apt-get install -y apt-transport-https software-properties-common && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    apt update && \
    apt install -y r-base r-base-dev libcurl4-openssl-dev libssl-dev libxml2-dev

RUN R -e "install.packages(c('data.table', 'feather', 'fst', 'readr', 'dplyr'), repos='https://rweb.crmda.ku.edu/cran/')"

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
