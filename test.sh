#!/usr/bin/env bash

r(){
#    echo -n "@$BASH_SOURCE:${BASH_LINENO[-2]}:"
    local expected=`sed "$((BASH_LINENO[-2] + 1))q;d" $BASH_SOURCE`

    echo '$' "$@"
    if [[ "$expected" =~ ^#\> ]];then
        expected=`echo "$expected" | sed -re 's/^#>\s*//'`
        local actual="$(redis-cli --no-raw "$@")"
        echo $actual
        if [[ "$expected" != "$actual" ]]; then
            echo -e "FAILED: Expected $expected"
        fi
    else
        redis-cli "$@"
    fi

}

r flushall

r ft.create idx
r ft.index idx a "redisbleve - Fulltext search module build on top of bleve"
r ft.index idx b "bleve - A modern text indexing library for go"
r ft.query idx bleve
r ft.query idx search
r ft.count idx
r ft.del idx b
r ft.query idx bleve
r ft.count idx
r del idx
