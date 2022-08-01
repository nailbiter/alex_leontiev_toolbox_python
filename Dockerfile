FROM python:3.9-buster

# download and install a dependency
#suggested by https://stackoverflow.com/a/60086958
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
RUN apt-get update
RUN apt-get -y install git

#RUN apt install -yy pandoc
#COPY ./requirements.txt .
#RUN pip3 install 

RUN mkdir alex_leontiev_toolbox_python
COPY pyproject.toml alex_leontiev_toolbox_python
COPY alex_leontiev_toolbox_python alex_leontiev_toolbox_python

CMD ["true"]
