#!/bin/bash -e

KEY=$(echo $1 | base64 -w 0)
VERSION=$2

function promise {
cat << SQL | ssh $1 sqlite3 paxosdb.sqlite3
create table if not exists paxos(
    key          text,
    version      integer,
    promised_seq integer,
    accepted_seq integer,
    session_uuid text,
    value        text,
    primary key(key, version));

insert or ignore into paxos values('$2',$3,0,0,null,null);

update paxos
set promised_seq=$4, session_uuid='$5'
where key='$2' and version=$3 and promised_seq < $4;

select promised_seq, accepted_seq, session_uuid, value
from paxos
where key='$2' and version=$3 and promised_seq=$4 and session_uuid='$5';

SQL
}

function accept {
cat << SQL | ssh $1 sqlite3 paxosdb.sqlite3

update paxos
set accepted_seq=$4, value='$6'
where key='$2' and version=$3 and promised_seq=$4 and session_uuid='$5';

delete from paxos
where key='$2' and version < (
    select max(version)
    from paxos
    where accepted_seq is not null);

SQL
}

function fetch {
cat << SQL | ssh $1 sqlite3 paxosdb.sqlite3

select version, value
from paxos
where key='$2' and accepted_seq is not null
order by version desc
limit 1

SQL
}

NODE_COUNT=$(echo $PAXOSDB_CLUSTER | wc -w)
QUORUM=$(($NODE_COUNT / 2 + 1))
1>&2 echo "quorum($QUORUM) nodes($NODE_COUNT)"

if [ $# -eq 2 ]; then
    VALUE=$(base64 -w 0 -)
    SESSION_UUID=$(uuid -v 4)
    PROPOSAL_SEQ=$(date +%s)

    1>&2 echo "proposal_seq($PROPOSAL_SEQ) session_uuid($SESSION_UUID)"

    seq=0
    count=0
    for NODE in $PAXOSDB_CLUSTER; do
        result=$(promise $NODE $KEY $VERSION $PROPOSAL_SEQ $SESSION_UUID)
        promised_seq=$(echo $result | cut -d'|' -f1)
        accepted_seq=$(echo $result | cut -d'|' -f2)
        session_uuid=$(echo $result | cut -d'|' -f3)

	1>&2 echo "promise($NODE) accepted_seq($accepted_seq)"
        if [ $promised_seq -eq $PROPOSAL_SEQ ]; then
            if [ "$session_uuid" = $SESSION_UUID ]; then
                count=$((count+1))
                if [ $accepted_seq -gt $seq ]; then
                    seq=$accepted_seq
                    VALUE=$(echo $result | cut -d'|' -f4)
                fi
            fi
        fi
    done

    if [ $count -ge $QUORUM ]; then
	1>&2 echo "accepted_seq($SEQ) accepted_value($VALUE)"

        for NODE in $PAXOSDB_CLUSTER; do
            accept $NODE $KEY $VERSION $PROPOSAL_SEQ $SESSION_UUID $VALUE
	    1>&2 echo "accept($NODE)"
        done

        exit 0
    fi
elif [ $# -eq 1 ]; then
    RESULT=$(for NODE in $PAXOSDB_CLUSTER; do
                 fetch $NODE $KEY
             done | sort | uniq -c)

    if [ $(echo $RESULT | wc -l) -eq 1 ]; then
        if [ $(echo $RESULT | cut -d' ' -f1) -ge $QUORUM ]; then
            ROW=$(echo $RESULT | cut -d' ' -f2)

            echo $ROW | cut -d'|' -f1
            echo $ROW | cut -d'|' -f2 | base64 -d
            exit 0
        fi
    fi
fi

exit 1
