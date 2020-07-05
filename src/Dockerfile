FROM ubuntu:latest

# Env variables
ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive
WORKDIR ${HOME}

# Root certificates update
RUN apt-get update -y && \
    apt-get install -yq ca-certificates && \
    update-ca-certificates

# Install dependencies
RUN apt-get install -yq \
    curl \
    gnupg \
    git \
    python3 \
    python3-pip

# Install NodeJS and Yarn
RUN curl -sL https://deb.nodesource.com/setup_12.x  | bash -
RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list
RUN apt update && apt-get install -yq yarn

# Install Python dependencies
COPY ./requirements.txt ${HOME}/requirements.txt
RUN python3 -m pip install -r requirements.txt && rm requirements.txt

# Install NodeJS dependencies
COPY ./package.json ${HOME}/package.json
RUN yarn && rm package.json

# Install Headless Chrome dependencies
RUN apt-get install -yq \
    gconf-service libasound2 libatk1.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libexpat1 \
    libfontconfig1 libgcc1 libgconf-2-4 libgdk-pixbuf2.0-0 libglib2.0-0 libgtk-3-0 libnspr4 \
    libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 \
    libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 \
    fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils libgbm-dev

# Install locales
RUN apt-get install -yq tzdata locales && \
    locale-gen en_US.UTF-8 && \
    locale-gen es_ES.UTF-8 && \
    dpkg-reconfigure --frontend noninteractive locales && \
    locale -a
ENV LC_ALL="en_US.UTF-8"
ENV LC_CTYPE="en_US.UTF-8"
