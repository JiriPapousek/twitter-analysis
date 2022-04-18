#!/bin/bash

function deploy_plugins {
        # NOTE: place here the commands to install Kafka Connect plugins, e.g.:
	confluent-hub install --no-prompt jcustenborder/kafka-connect-twitter:0.3.34
}

function deploy_connectors {
	deploy 'Twitter%20source%20connector' '{
        "connector.class": "com.github.jcustenborder.kafka.connect.twitter.TwitterSourceConnector",
        "twitter.oauth.accessTokenSecret": "",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "process.deletes": "true",
        "filter.keywords": "ukraine",
        "kafka.status.topic": "tweets",
        "name": "Twitter source connector",
        "kafka.delete.topic": "tweets-delete",
        "twitter.oauth.consumerSecret": "",
        "twitter.oauth.accessToken": "",
        "twitter.oauth.consumerKey": ""
        }'
}

function deploy {
        curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/connectors/$1/config -d "$2"
}

if [ ! -f /usr/share/confluent-hub-components/initialized ]; then
        touch /usr/share/confluent-hub-components/initialized

        echo "Kafka Connect initialization started"
        deploy_plugins

        function wait_started_and_deploy_connectors() {
                while : ; do
                        status=`curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors`
                        [ $status -eq 200 ] && break
                        sleep 1
                done
                echo "Kafka Connect started"
                deploy_connectors
                echo "Kafka Connect initialization completed"
        }

        wait_started_and_deploy_connectors &  # run in parallel to exec
fi

exec /etc/confluent/docker/run  # original command to start Kafka Connect (CMD directive)

