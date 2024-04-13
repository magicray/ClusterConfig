#!/bin/bash

NODES=$(cat $1)
KEY=$(echo $2 | base64 -w 0)
VERSION=$3

QUORUM=$(($(echo $NODES | wc -w) / 2 + 1))
KEY_VERSION=$(printf "%s-%014d" $KEY $VERSION)
PROPOSAL_SEQ=$(date +%Y%m%d%H%M%S)

DEFAULT_FILE=$(printf "%s-%014d-%014d" $KEY_VERSION 0 0)
KEY_VERSION_PROPOSAL_SEQ=$(printf "%s-%014d" $KEY_VERSION $PROPOSAL_SEQ)


function fetch {
cat << BASHSCRIPT | ssh $1 /usr/bin/bash
set -e

mkdir -p paxosdb && cd paxosdb
FILE=\$(ls | grep -E "^$KEY" | grep -v '\-00000000000000\-' | sort | tail -n 1)

echo -n BEGIN-\$FILE-
cat \$FILE
echo '-END'

BASHSCRIPT
}


function paxos_promise {
cat << BASHSCRIPT | ssh $1 /usr/bin/bash
set -e

mkdir -p paxosdb && cd paxosdb && touch .lockfile
exec 9>.lockfile && flock -x -n 9

touch $DEFAULT_FILE
OLDFILE=\$(ls | grep -E "^$KEY_VERSION" | sort | tail -n 1)
PROMISED_SEQ=\$(echo \$OLDFILE | cut -d- -f3)

if [ $PROPOSAL_SEQ -gt \$PROMISED_SEQ ]; then
    ACCEPTED_SEQ=\$(echo \$OLDFILE | cut -d- -f4)
    NEWFILE=\$(printf "%s-%014d" $KEY_VERSION_PROPOSAL_SEQ \$ACCEPTED_SEQ)
    mv \$OLDFILE \$NEWFILE

    echo -n BEGIN-\$NEWFILE-
    cat \$NEWFILE
    echo '-END'
fi
BASHSCRIPT
}


function paxos_accept {
cat << BASHSCRIPT | ssh $1 /usr/bin/bash
set -e

if [ ! $2 ]; then
    echo \$USER@\$HOSTNAME empty value not allowed
    exit 1
fi

mkdir -p paxosdb && cd paxosdb && touch .lockfile
exec 9>.lockfile && flock -x -n 9

touch $DEFAULT_FILE
OLDFILE=\$(ls | grep -E "^$KEY_VERSION" | sort | tail -n 1)
PROMISED_SEQ=\$(echo \$OLDFILE | cut -d- -f3)

if [ $PROPOSAL_SEQ -ge \$PROMISED_SEQ ]; then
    NEWFILE=\$(printf "%s-%014d" $KEY_VERSION_PROPOSAL_SEQ $PROPOSAL_SEQ)
    echo -n $2 > tmp.\$NEWFILE.tmp
    mv tmp.\$NEWFILE.tmp \$NEWFILE

    rm \$(ls -r | grep -E "^$KEY-" | tail -n +2)

    echo \$USER@\$HOSTNAME accepted \$NEWFILE \$(cat \$NEWFILE | wc -c)
fi
BASHSCRIPT
}


if [ $# -gt 2 ]; then
    VALUE=$(base64 -w 0 -)
    TMPFILE=$(mktemp -p /var/tmp)

    for NODE in $NODES; do
        paxos_promise $NODE | grep -E "^BEGIN-.+-.+-.+-.+-.*-END$"
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
        paxos_accept $NODE $VALUE
    done
else
    TMPFILE=$(mktemp -p /var/tmp)

    for NODE in $NODES; do
        fetch $NODE | grep $KEY | grep -E "^BEGIN-.+-.+-.+-.+-.+-END$"
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
