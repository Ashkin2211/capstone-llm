FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.4-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

#TODO add your project code and dependencies to the image
#Copying my project files to the container
COPY . /opt/spark/work-dir

#Install all requirements
RUN python3 -m pip install -r requirements.txt

ENTRYPOINT [ "ingest.py" ]
CMD [ "main.py" ]
