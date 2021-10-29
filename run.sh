#!/bin/bash
#beautification
border_title() {
    local str="$*" # Put all arguments into single string
    local len=${#str}
    local i
    for ((i = 0; i < len + 4; ++i)); do
        printf '-'
    done
    printf "\n| $str |\n"
    for ((i = 0; i < len + 4; ++i)); do
        printf '-'
    done
    echo
}
#main loop
while :; do
    clear
    border_title 'Welcome to the SearchMyData App'
    echo ' 1--Run application'
    echo ' 2--Install dependencies'
    echo ' 3--Exit'
    for ((i = 0; i < 35; i++)); do
        printf '-'
    done
    echo
    echo -n 'Enter your choice: '
    read option
    if [[ $option -eq 1 ]]; then
        echo 'Application is starting...'
        sleep 2
        python searchmydata.py
    elif [[ $option -eq 2 ]]; then
        echo 'Installing dependencies...'
        python --version
        pip install -r requirements.txt
        echo 'Everything is done'
        sleep 3
    elif [[ $option -eq 3 ]]; then
        exit
    else
        echo 'Input error'
        sleep 2
    fi
done