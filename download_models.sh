#!/bin/bash

MODELS_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )"/models

function download {
    URL=$1
    FILE=`basename $URL`
    DIR=${FILE%.zip}
    if [ ! -d "$DIR" ]; then
        echo "Downloading $URL"
        mkdir $DIR
        cd $DIR
        curl -s -O $URL
        unzip -q $FILE
        rm $FILE
        cd ..
    fi
}

mkdir -p $MODELS_DIR
cd $MODELS_DIR

download 'https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/models/translate_mul_en_xx_3.1.0_2.4_1622843259436.zip'

# download other models here, if needed

