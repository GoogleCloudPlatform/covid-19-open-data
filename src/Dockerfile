FROM gcr.io/google-appengine/python

# Env variables
ENV HOME /root
WORKDIR ${HOME}

# Create a virtualenv for dependencies
RUN virtualenv /env

# Equivalent to source /env/bin/activate
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

# Install our app requirements
ADD ./requirements.txt ${HOME}/requirements.txt
RUN python3 -m pip install -r requirements.txt && rm requirements.txt

# Add the application source code
ADD . ${HOME}

# Run a WSGI server to serve the application
CMD gunicorn -b :$PORT appengine:app --timeout 3600
