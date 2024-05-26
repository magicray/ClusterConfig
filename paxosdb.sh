#!/bin/bash -e

KEY=$(echo $1 | base64 -w 0)
VERSION=$2

NODE_COUNT=$(echo $PAXOSDB_CLUSTER | wc -w)
QUORUM=$(($NODE_COUNT / 2 + 1))


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

select session_uuid, accepted_seq, value, session_uuid
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

select key, version, value, key
from paxos
where key='$2' and accepted_seq is not null
order by version desc
limit 1

SQL
}

function write {
    local KEY=$1
    local VERSION=$2
    local VALUE=$3

    local SESSION_UUID=$(uuid -v 4)
    local PROPOSAL_SEQ=$(date +%s)
    local MD5=$(echo $VALUE | base64 -d | md5sum | cut -d' ' -f1)

    1>&2 echo "quorum($QUORUM) nodes($NODE_COUNT) value($MD5)"
    1>&2 echo "proposal($PROPOSAL_SEQ) session($SESSION_UUID)"

    local seq=0
    local count=0
    for NODE in $PAXOSDB_CLUSTER; do
        result=$(promise $NODE $KEY $VERSION $PROPOSAL_SEQ $SESSION_UUID)
        local prefix=$(echo $result | cut -d'|' -f1)
        local suffix=$(echo $result | cut -d'|' -f4)

        if [ $prefix = $SESSION_UUID ] && [ $suffix = $SESSION_UUID ]; then
            count=$((count+1))
            local accepted_seq=$(echo $result | cut -d'|' -f2)
            local accepted_value=$(echo $result | cut -d'|' -f3)

            MD5=$(echo $accepted_value | base64 -d | md5sum | cut -d' ' -f1)
	    1>&2 echo "promise($NODE) accepted_seq($accepted_seq) value($MD5)"

            if [ $accepted_seq -gt $seq ]; then
                seq=$accepted_seq
                VALUE=$accepted_value
            fi
        fi
    done

    if [ $count -ge $QUORUM ]; then
        local MD5=$(echo $accepted_value | base64 -d | md5sum | cut -d' ' -f1)
	1>&2 echo "accepted_seq($seq) accepted_value($MD5)"

        for NODE in $PAXOSDB_CLUSTER; do
            accept $NODE $KEY $VERSION $PROPOSAL_SEQ $SESSION_UUID $VALUE
	    1>&2 echo "accept($NODE)"
        done
    fi
}

if [ $# -eq 2 ]; then
    write $KEY $VERSION $(base64 -w 0 -)
elif [ $# -eq 1 ]; then
    RESULT=$(for NODE in $PAXOSDB_CLUSTER; do
                 fetch $NODE $KEY
             done | sort | uniq -c)

    if [ $(echo $RESULT | wc -l) -eq 1 ]; then
        if [ $(echo $RESULT | cut -d' ' -f1) -ge $QUORUM ]; then
            ROW=$(echo $RESULT | cut -d' ' -f2)

            echo $ROW | cut -d'|' -f2
            echo $ROW | cut -d'|' -f3 | base64 -d
            exit 0
        fi
    fi
    exit 1
fi
