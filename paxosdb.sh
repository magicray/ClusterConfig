#!/bin/bash

NODES=$(cat $1)
KEY=$(echo $2 | base64 -w 0)
VERSION=$3

QUORUM=$(($(echo $NODES | wc -w) / 2 + 1))
KEY_VERSION=$(printf "%s-%014d" $KEY $VERSION)
PROPOSAL_SEQ=$(date +%Y%m%d%H%M%S)

PAXOS_DIR=paxosdb/$(echo $KEY | md5sum | cut -c -3)
KEY_VERSION_PROPOSAL_SEQ=$(printf "%s-%014d" $KEY_VERSION $PROPOSAL_SEQ)

function read_server {
cat << BASHSCRIPT | ssh $1 /usr/bin/bash -e

cd $PAXOS_DIR

FILE=\$(ls | grep -E "^$KEY" | grep -v '\-00000000000000\-' | sort | tail -n 1)

echo BEGIN-\$FILE-\$(cat \$FILE)-END

BASHSCRIPT
}


function paxos_server {
cat << BASHSCRIPT | ssh $1 /usr/bin/bash -e

mkdir -p $PAXOS_DIR && cd $PAXOS_DIR
touch .lockfile && exec 9>.lockfile && flock -x -n 9

touch \$(printf "%s-%014d-%014d" $KEY_VERSION 0 0)

OLDFILE=\$(ls | grep -E "^$KEY_VERSION" | sort | tail -n 1)

PROMISED_SEQ=\$(echo \$OLDFILE | cut -d- -f3)
ACCEPTED_SEQ=\$(echo \$OLDFILE | cut -d- -f4)

# PAXOS - Promise Phase
if [ ! $2 ] && [ $PROPOSAL_SEQ -gt \$PROMISED_SEQ ]; then
    NEWFILE=\$(printf "%s-%014d" $KEY_VERSION_PROPOSAL_SEQ \$ACCEPTED_SEQ)

    mv \$OLDFILE \$NEWFILE

    echo BEGIN-\$NEWFILE-\$(cat \$NEWFILE)-END
fi

# PAXOS - Accept Phase
if [ $2 ] && [ $PROPOSAL_SEQ -ge \$PROMISED_SEQ ]; then
    NEWFILE=\$(printf "%s-%014d" $KEY_VERSION_PROPOSAL_SEQ $PROPOSAL_SEQ)

    echo -n $2 > .tmp && mv .tmp \$NEWFILE

    echo \$USER@\$HOSTNAME accepted \$NEWFILE
fi

if [ -s \$(ls -r | grep -E "^$KEY-" | head -n 1) ]; then
    rm \$(ls -r | grep -E "^$KEY-" | tail -n +2)
fi

BASHSCRIPT
}


if [ $# -gt 2 ]; then
    VALUE=$(base64 -w 0 -)
    TMPFILE=$(mktemp -p /var/tmp)

    for NODE in $NODES; do
        paxos_server $NODE | grep -E "^BEGIN-.+-.+-.+-.+-.*-END$"
    done | grep $KEY_VERSION | sort > $TMPFILE

    if [ ! -s $TMPFILE ] || [ $QUORUM -gt $(cat $TMPFILE | wc -l) ]; then
        rm $TMPFILE
        echo NO_QUORUM
        exit 1
    fi

    LASTLINE=$(tail -n 1 $TMPFILE)
    rm $TMPFILE

    if [ $(echo $LASTLINE | cut -d- -f5) -gt 0 ]; then
        VALUE=$(echo $LASTLINE | cut -d- -f6)
        echo writing EXISTING value
    else
        echo writing NEW value
    fi

    for NODE in $NODES; do
        paxos_server $NODE $VALUE
    done
else
    TMPFILE=$(mktemp -p /var/tmp)

    for NODE in $NODES; do
        read_server $NODE | grep $KEY | grep -E "^BEGIN-.+-.+-.+-.+-.+-END$"
    done | sort > $TMPFILE

    if [ ! -s $TMPFILE ]; then
        rm $TMPFILE
        echo NOT_FOUND
        exit 1
    fi

    KEY_VERSION=$(tail -n 1 $TMPFILE | cut -d- -f2-3)
    VALUE=$(cat $TMPFILE | grep $KEY_VERSION | cut -d- -f6 | uniq -c)
    rm $TMPFILE

    if [ $QUORUM -gt $(echo $VALUE | cut -d' ' -f1) ]; then
        echo NO_QUORUM
        exit 2
    fi

    echo version=$(($(echo $KEY_VERSION | cut -d- -f2)*1))
    echo $VALUE | cut -d' ' -f2 | base64 -d
fi
