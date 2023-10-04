FROM rocker/rstudio:4.3.1

RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    libgit2-dev \
    openssh-client \
    zlib1g -y \
    zlib1g-dev -y \
    libpng-dev -y \
    libgdal-dev -y \
    cmake
    

# RUN mkdir -p /home/rstudio/workspace/shiny_app
# WORKDIR /home/rstudio/workspace/shiny_app
# EXPOSE 1313
