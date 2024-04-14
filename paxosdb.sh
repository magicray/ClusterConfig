function paxosdb_read_server {
# 1-NODE, 2-KEY

cat << BASHSCRIPT | ssh $1 /usr/bin/bash -e
mkdir -p PaxosDB && cd PaxosDB

FILE=\$(ls | grep -E "^$2\-" | grep -v '\-00000000000000\-' | sort | tail -n 1)

echo BEGIN-\$FILE-\$(cat \$FILE)-END
BASHSCRIPT
}


function paxosdb_write_server {
# 1-NODE, 2-KEY, 3-VERSION, 4-PROPOSAL_SEQ, 5-VALUE

cat << BASHSCRIPT | ssh $1 /usr/bin/bash -e
mkdir -p PaxosDB && cd PaxosDB
touch .lockfile && exec 9>.lockfile && flock -x 9

touch \$(printf "%s-%014d-%014d-%014d" $2 0 0 0)

OLDFILE=\$(ls | grep -E "^$2\-" | sort | tail -n 1)
LATEST_VER=\$(echo \$OLDFILE | cut -d- -f2)
PROMISED_SEQ=\$(echo \$OLDFILE | cut -d- -f3)
ACCEPTED_SEQ=\$(echo \$OLDFILE | cut -d- -f4)

if [ $3 -lt \$LATEST_VER ]; then
    PROMISED_SEQ=99999999999999
    ACCEPTED_SEQ=99999999999999
fi

if [ $3 -gt \$LATEST_VER ]; then
    PROMISED_SEQ=00000000000000
    ACCEPTED_SEQ=00000000000000
fi

if [ $# -eq 4 ] && [ $4 -gt \$PROMISED_SEQ ]; then
    FILE=\$(printf "%s-%s-%s-%s" $2 $3 $4 \$ACCEPTED_SEQ)

    mv \$OLDFILE \$FILE

    echo BEGIN-\$FILE-\$(cat \$FILE)-END
fi

if [ $# -eq 5 ] && [ $4 -ge \$PROMISED_SEQ ]; then
    FILE=\$(printf "%s-%s-%s-%s" $2 $3 $4 $4)

    echo -n $5 > .tmp && mv .tmp \$FILE

    echo \$USER@\$HOSTNAME accepted \$FILE

    FLAG=0
    for FILE in \$(ls -r | grep -E "^$2\-"); do
        [ \$FLAG -gt 0 ] && rm \$FILE

        [ -s \$FILE ] && FLAG=1
    done
fi
BASHSCRIPT
}


function paxosdb {
    local key=$(echo $1 | base64 -w 0)
    local version=$(printf "%014d" $2)

    local QUORUM=$(($(echo $PAXOSDB_CLUSTER | wc -w) / 2 + 1))
    local tmpfile=$(mktemp -p /var/tmp)
    local proposal_seq=$(date +%Y%m%d%H%M%S)

    if [ $# -gt 1 ]; then
        for NODE in $PAXOSDB_CLUSTER; do
            paxosdb_write_server $NODE $key $version $proposal_seq
	done | grep -E "^BEGIN\-$key\-$version\-.+\-.+\-.*-END$" | sort > $tmpfile

        if [ $(cat $tmpfile | wc -l) -ge $QUORUM ]; then
            local lastline=$(tail -n 1 $tmpfile)
            local accepted_seq=$(echo $lastline | cut -d- -f5)
            local accepted_value=$(echo $lastline | cut -d- -f6)

            if [ $accepted_seq -gt 0 ]; then
                local proposal_value=$accepted_value
                echo writing EXISTING value
            else
                local proposal_value=$(base64 -w 0 -)
                echo writing NEW value
            fi

            for NODE in $PAXOSDB_CLUSTER; do
                paxosdb_write_server $NODE $key $version $proposal_seq $proposal_value
            done
        fi
    else
        echo BEGIN-$key-00000000000000-0-0--END > $tmpfile
        for NODE in $PAXOSDB_CLUSTER; do
            paxosdb_read_server $NODE $key | grep -E "^BEGIN\-$key\-.+\-.+\-.+\-.+-END$"
        done | sort >> $tmpfile

        local key_version=$(tail -n 1 $tmpfile | cut -d- -f2-3)
        local count_value=$(cat $tmpfile | grep $key_version | cut -d- -f6 | uniq -c)

        if [ $(echo $count_value | cut -d' ' -f1) -ge $QUORUM ]; then
            echo KEY: $(echo $key_version | cut -d- -f1 | base64 -d)
            echo VERSION: $((10#$(echo $key_version | cut -d- -f2)))
            echo
            echo $count_value | cut -d' ' -f2 | base64 -d
        fi
    fi

    rm $tmpfile
}
