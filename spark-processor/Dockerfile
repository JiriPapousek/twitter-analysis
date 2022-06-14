# We start from the Docker image of Spark (the one packaged by Bitnami, including Python3), so to have access to spark-submit
FROM bitnami/spark:3.2.1

# comma-separated list of 'packages' we depend on
ENV PROCESSOR_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.3.5,com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.4

# Switch to user root to perform some admin stuff (incl. installing libraries)
USER root

# embed Spark NLP and all its dependencies in the Docker image, to avoid downloading them every time a container is started
# (note: this is done by submitting a dummy empty python script, using spark.jars.ivy to tell spark-submit where to put downloaded jars)
RUN mkdir /opt/bitnami/spark/.ivy2 && \
    touch /tmp/dummy.py && \
    spark-submit --conf spark.jars.ivy=/opt/bitnami/spark/.ivy2 --packages ${PROCESSOR_PACKAGES} /tmp/dummy.py && \
    rm /tmp/dummy.py && \
    chown -R 1001 /opt/bitnami/spark/.ivy2

# Create directory /app and set it as working directory for all next instructions
WORKDIR /app

# Copy requirements.txt individually and install deps (as long as requirements.txt does not change, Docker cache is reused)
COPY requirements.txt .

# Prepare a tar archive containing virtualenv to be submitted with the processor script to Spark cluster
RUN python -m venv pyspark_venv 
RUN . pyspark_venv/bin/activate && pip install -U pip && pip install -r requirements.txt
RUN . pyspark_venv/bin/activate && python -m spacy download en_core_web_sm
RUN . pyspark_venv/bin/activate && python -c "import nltk;nltk.download('maxent_ne_chunker');nltk.download('words');nltk.download('treebank');nltk.download('maxent_treebank_pos_tagger');nltk.download('punkt');nltk.download('averaged_perceptron_tagger')"
RUN . pyspark_venv/bin/activate && python -c "import locationtagger; le = locationtagger.LocationExtractor(None); le.populate_db()"
RUN . pyspark_venv/bin/activate && cp -r /nltk_data pyspark_venv/nltk_data
RUN . pyspark_venv/bin/activate && pip install venv-pack && venv-pack -o pyspark_venv.tar.gz

ENV PYSPARK_DRIVER_PYTHON=python
ENV PYSPARK_PYTHON=./environment/bin/python

RUN chown 1001:1001 pyspark_venv.tar.gz

# Copy source files next (any change will invalidate Docker cache causing the rerun of next instructions)
COPY . .

# Switch back to the user configured in the Bitnami image to run Spark
USER 1001

# We submit the Spark application on a configurable Spark cluster
# The cluster can be created using this very same image (as it is based on the Bitnami Spark one).
# The default property values are set assuming the container is run with --network=host, so all required
# services are available on localhost at usual ports
#
# NOTE: if there are additional python files besides the main script, list them with option --py-files
# (see help of spark-submit)
#

CMD . pyspark_venv/bin/activate && spark-submit \
    --archives pyspark_venv.tar.gz#environment \
    --master ${PROCESSOR_MASTER:-spark://localhost:7077} \
    --packages ${PROCESSOR_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2} \
    /app/${PROCESSOR_IMPLEMENTATION:-processor.py} \
    --bootstrap-servers ${PROCESSOR_BOOTSTRAP_SERVERS:-localhost:29092} \
    $PROCESSOR_ARGS
