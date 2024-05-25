#!/bin/bash -e

KEY=$(echo $1 | base64 -w 0)
VERSION=$2
QUORUM=$(($(echo $PAXOS_CLUSTER | wc -w) / 2 + 1))


function promise {
cat << SQL | ssh $1 sqlite3 ./$PAXOS_DB
create table if not exists paxos(
    key          text,
    version      integer,
    promised_seq integer,
    accepted_seq integer,
    value        text,
    primary key(key, version));

insert or ignore into paxos values('$2',$3,0,0,null);

update paxos
set promised_seq=$4
where key='$2' and version=$3 and promised_seq < $4;

select accepted_seq, value
from paxos
where key='$2' and version=$3 and promised_seq=$4;

SQL
}

function accept {
cat << SQL | ssh $1 sqlite3 ./$PAXOS_DB

update paxos
set promised_seq=$4, accepted_seq=$4, value='$5'
where key='$2' and version=$3 and promised_seq=$4;

delete from paxos where key='$2' and version < (
    select max(version) from paxos where accepted_seq is not null);

SQL
}

function fetch {
cat << SQL | ssh $1 sqlite3 ./$PAXOS_DB

select version, accepted_seq, value
from paxos
where key='$2' and accepted_seq is not null
order by version desc
limit 1

SQL
}

1>&2 echo "db($PAXOS_DB) quorum($QUORUM)"
for NODE in $PAXOS_CLUSTER; do
    1>&2 echo "nodes($NODE)"
done
1>&2 echo

if [ $# -eq 2 ]; then
    PROPOSAL_SEQ=$(date +%s)

    ACCEPTED_SEQ=0
    ACCEPTED_VALUE=$(base64 -w 0 -)

    for NODE in $PAXOS_CLUSTER; do
        result=$(promise $NODE $KEY $VERSION $PROPOSAL_SEQ)
        if [ $(echo $result | cut -d'|' -f1) -gt $ACCEPTED_SEQ ]; then
            ACCEPTED_VALUE=$(echo $result | cut -d'|' -f2)
        fi
    done

    echo "proposing for accepted_seq($ACCEPTED_SEQ) value($ACCEPTED_VALUE)"

    for NODE in $PAXOS_CLUSTER; do
        accept $NODE $KEY $VERSION $PROPOSAL_SEQ $ACCEPTED_VALUE
    done
elif [ $# -eq 1 ]; then
    RESULT=$(for NODE in $PAXOS_CLUSTER; do
                 fetch $NODE $KEY
             done | sort | uniq -c)

    if [ $RESULT ] && [ $(echo $RESULT | wc -l) -eq 1 ]; then
        if [ $(echo $RESULT | cut -d' ' -f1) -ge $QUORUM ]; then
            ROW=$(echo $RESULT | cut -d' ' -f2)
            VERSION=$(echo $ROW | cut -d'|' -f1)
            VALUE=$(echo $ROW | cut -d'|' -f3)
            echo $VERSION
            echo $VALUE | base64 -d
        fi
    fi
fi
