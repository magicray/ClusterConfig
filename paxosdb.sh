function paxosdb_read_server {
KEY=$2

cat << BASHSCRIPT | ssh $1 /usr/bin/bash -e
mkdir -p PaxosDB && cd PaxosDB

FILE=\$(ls | grep -E "^$KEY-" | grep -v '\-00000000000000\-' | sort | tail -n 1)

echo BEGIN-\$FILE-\$(cat \$FILE)-END
BASHSCRIPT
}


function paxosdb_write_server {
KEY=$2
VERSION=$3
PROPOSAL_SEQ=$4

cat << BASHSCRIPT | ssh $1 /usr/bin/bash -e
mkdir -p PaxosDB && cd PaxosDB
touch .lockfile && exec 9>.lockfile && flock -x 9

touch \$(printf "%s-%014d-%014d-%014d" $KEY 0 0 0)

OLDFILE=\$(ls | grep -E "^$KEY\-" | sort | tail -n 1)
LATEST_VER=\$(echo \$OLDFILE | cut -d- -f2)
PROMISED_SEQ=\$(echo \$OLDFILE | cut -d- -f3)
ACCEPTED_SEQ=\$(echo \$OLDFILE | cut -d- -f4)

if [ $VERSION -lt \$LATEST_VER ]; then
    PROMISED_SEQ=99999999999999
    ACCEPTED_SEQ=99999999999999
fi

if [ $VERSION -gt \$LATEST_VER ]; then
    PROMISED_SEQ=00000000000000
    ACCEPTED_SEQ=00000000000000
fi

if [ $# -eq 4 ] && [ $PROPOSAL_SEQ -gt \$PROMISED_SEQ ]; then
    FILE=\$(printf "%s-%s-%s-%s" $KEY $VERSION $PROPOSAL_SEQ \$ACCEPTED_SEQ)

    mv \$OLDFILE \$FILE

    echo BEGIN-\$FILE-\$(cat \$FILE)-END
fi

if [ $# -eq 5 ] && [ $PROPOSAL_SEQ -ge \$PROMISED_SEQ ]; then
    FILE=\$(printf "%s-%s-%s-%s" $KEY $VERSION $PROPOSAL_SEQ $PROPOSAL_SEQ)

    echo -n $5 > .tmp && mv .tmp \$FILE

    echo \$USER@\$HOSTNAME accepted \$FILE

    FLAG=0
    for FILE in \$(ls -r | grep -E "^$KEY\-"); do
        [ \$FLAG -gt 0 ] && rm \$FILE && echo \$USER@\$HOSTNAME deleted \$FILE

        [ -s \$FILE ] && FLAG=1
    done
fi
BASHSCRIPT
}


function paxosdb {
    KEY=$(echo $1 | base64 -w 0)
    VERSION=$(printf "%014d" $2)

    QUORUM=$(($(echo $PAXOSDB_CLUSTER | wc -w) / 2 + 1))
    TMPFILE=$(mktemp -p /var/tmp)
    PROPOSAL_SEQ=$(date +%Y%m%d%H%M%S)

    if [ $# -gt 1 ]; then
        for NODE in $PAXOSDB_CLUSTER; do
            paxosdb_write_server $NODE $KEY $VERSION $PROPOSAL_SEQ
	done | grep -E "^BEGIN\-$KEY\-$VERSION\-.+\-.+\-.*-END$" | sort > $TMPFILE

        if [ $(cat $TMPFILE | wc -l) -ge $QUORUM ]; then
            LASTLINE=$(tail -n 1 $TMPFILE)
            ACCEPTED_SEQ=$(echo $LASTLINE | cut -d- -f5)
            ACCEPTED_VALUE=$(echo $LASTLINE | cut -d- -f6)

            if [ $ACCEPTED_SEQ -gt 0 ]; then
                PROPOSAL_VALUE=$ACCEPTED_VALUE
                echo writing EXISTING value
            else
                PROPOSAL_VALUE=$(base64 -w 0 -)
                echo writing NEW value
            fi

            for NODE in $PAXOSDB_CLUSTER; do
                paxosdb_write_server $NODE $KEY $VERSION $PROPOSAL_SEQ $PROPOSAL_VALUE
            done
        fi
    else
        echo BEGIN-$KEY-00000000000000-0-0--END > $TMPFILE
        for NODE in $PAXOSDB_CLUSTER; do
            paxosdb_read_server $NODE $KEY | grep -E "^BEGIN\-$KEY\-.+\-.+\-.+\-.+-END$"
        done | sort >> $TMPFILE

        KEY_VERSION=$(tail -n 1 $TMPFILE | cut -d- -f2-3)
        COUNT_VALUE=$(cat $TMPFILE | grep $KEY_VERSION | cut -d- -f6 | uniq -c)

        if [ $(echo $COUNT_VALUE | cut -d' ' -f1) -ge $QUORUM ]; then
    	    echo KEY: $(echo $KEY_VERSION | cut -d- -f1 | base64 -d)
            echo VERSION: $((10#$(echo $KEY_VERSION | cut -d- -f2)))
            echo
            echo $COUNT_VALUE | cut -d' ' -f2 | base64 -d
        fi
    fi

    rm $TMPFILE
}
